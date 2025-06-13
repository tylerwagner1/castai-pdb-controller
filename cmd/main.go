package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "strconv"
    "strings"
    "sync"
    "syscall"
    "time"
    "log"
    "io"

    appsv1 "k8s.io/api/apps/v1"
    policyv1 "k8s.io/api/policy/v1"
    corev1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/labels"
    "k8s.io/apimachinery/pkg/util/intstr"
    "k8s.io/client-go/informers"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"
    "k8s.io/client-go/tools/cache"
    "k8s.io/client-go/tools/clientcmd"
    "k8s.io/client-go/tools/leaderelection"
    "k8s.io/client-go/tools/leaderelection/resourcelock"
    apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
    configMapNamespace = "castai-agent"
    configMapName      = "castai-pdb-controller-config"
    annotationMinAvailable   = "workloads.cast.ai/pdb-minAvailable"
    annotationMaxUnavailable = "workloads.cast.ai/pdb-maxUnavailable"
    annotationBypass         = "workloads.cast.ai/bypass-default-pdb"
    skipLogInterval          = 15 * time.Minute
    warnLogInterval = 15 * time.Minute // or whatever interval you prefer
)

type DefaultPDBConfig struct {
    MinAvailable   *intstr.IntOrString
    MaxUnavailable *intstr.IntOrString
    FixPoorPDBs    bool
}

var (
    defaultPDBConfig     DefaultPDBConfig
    defaultPDBConfigLock sync.RWMutex

    skipLogTimes     = make(map[string]time.Time)
    skipLogTimesLock sync.Mutex
    warnLogTimes     = make(map[string]time.Time)
    warnLogTimesLock sync.Mutex
)

func logPoorPDBWarning(key, message string) {
    now := time.Now()
    warnLogTimesLock.Lock()
    last, ok := warnLogTimes[key]
    if !ok || now.Sub(last) > warnLogInterval {
        log.Print(message)
        warnLogTimes[key] = now
    }
    warnLogTimesLock.Unlock()
}

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    config, err := rest.InClusterConfig()
    if err != nil {
        kubeconfig := os.Getenv("KUBECONFIG")
        config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
        if err != nil {
            panic(err)
        }
    }

    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        panic(err)
    }

    id, err := os.Hostname()
    if err != nil {
        panic(err)
    }

    // Silence logs in all pods until leadership is acquired
    log.SetOutput(io.Discard)

    loadDefaultPDBConfig(ctx, clientset)
    go watchConfigMap(ctx, clientset)

    // Scan all existing PDBs for poor configuration at startup
    go scanAllPDBsForPoorConfig(ctx, clientset)

    lock := &resourcelock.LeaseLock{
        LeaseMeta: metav1.ObjectMeta{
            Name:      "castai-pdb-controller-leader-election",
            Namespace: configMapNamespace,
        },
        Client: clientset.CoordinationV1(),
        LockConfig: resourcelock.ResourceLockConfig{
            Identity: id,
        },
    }

    leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
        Lock:            lock,
        LeaseDuration:   15 * time.Second,
        RenewDeadline:   10 * time.Second,
        RetryPeriod:     2 * time.Second,
        Callbacks: leaderelection.LeaderCallbacks{
            OnStartedLeading: func(ctx context.Context) {
                // Re-enable logging for the leader pod
                log.SetOutput(os.Stderr)
                log.Printf("[%s] I am the leader now\n", id)
                runController(ctx, clientset)
            },
            OnStoppedLeading: func() {
                log.Printf("[%s] Lost leadership, exiting\n", id)
                os.Exit(0)
            },
            OnNewLeader: func(identity string) {
                if identity == id {
                    log.Printf("[%s] I am the new leader\n", id)
                } else {
                    log.Printf("[%s] New leader elected: %s\n", id, identity)
                }
            },
        },
        ReleaseOnCancel: true,
        Name:            "castai-pdb-controller",
    })
}


func watchConfigMap(ctx context.Context, clientset *kubernetes.Clientset) {
    factory := informers.NewSharedInformerFactoryWithOptions(
        clientset,
        0,
        informers.WithNamespace(configMapNamespace),
    )
    cmInformer := factory.Core().V1().ConfigMaps().Informer()

    cmInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == configMapName {
                updateDefaultPDBConfig(cm, clientset)
            }
        },
        UpdateFunc: func(oldObj, newObj interface{}) {
            if cm, ok := newObj.(*corev1.ConfigMap); ok && cm.Name == configMapName {
                updateDefaultPDBConfig(cm, clientset)
            }
        },
        DeleteFunc: func(obj interface{}) {
            if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == configMapName {
                resetDefaultPDBConfig()
            }
        },
    })

    factory.Start(ctx.Done())
    factory.WaitForCacheSync(ctx.Done())
    <-ctx.Done()
}

func updateDefaultPDBConfig(cm *corev1.ConfigMap, clientset *kubernetes.Clientset) {
    var minAvailable, maxUnavailable *intstr.IntOrString
    if val, ok := cm.Data["minAvailable"]; ok {
        minAvailable = parsePDBValue(val)
    }
    if val, ok := cm.Data["maxUnavailable"]; ok {
        maxUnavailable = parsePDBValue(val)
    }
    fixPoorPDBs := false
    if val, ok := cm.Data["FixPoorPDBs"]; ok && strings.ToLower(val) == "true" {
        fixPoorPDBs = true
    }
    if minAvailable != nil && maxUnavailable != nil {
        log.Printf("Invalid default PDB config: both minAvailable and maxUnavailable set in ConfigMap\n")
        minAvailable = nil
        maxUnavailable = nil
    }
    defaultPDBConfigLock.Lock()
    defaultPDBConfig.MinAvailable = minAvailable
    defaultPDBConfig.MaxUnavailable = maxUnavailable
    defaultPDBConfig.FixPoorPDBs = fixPoorPDBs
    defaultPDBConfigLock.Unlock()
    log.Printf("Default PDB config updated from ConfigMap: minAvailable=%v, maxUnavailable=%v, FixPoorPDBs=%v\n", minAvailable, maxUnavailable, fixPoorPDBs)

    // Reconcile all PDBs that use defaults
    go reconcileAllDefaultPDBs(context.Background(), clientset)
    go scanAllPDBsForPoorConfig(context.Background(), clientset)
}

func resetDefaultPDBConfig() {
    defaultPDBConfigLock.Lock()
    defaultPDBConfig.MinAvailable = nil
    defaultPDBConfig.MaxUnavailable = nil
    defaultPDBConfig.FixPoorPDBs = false
    defaultPDBConfigLock.Unlock()
    log.Printf("Default PDB config reset: using built-in fallback\n")
}

func loadDefaultPDBConfig(ctx context.Context, clientset *kubernetes.Clientset) {
    cm, err := clientset.CoreV1().ConfigMaps(configMapNamespace).Get(ctx, configMapName, metav1.GetOptions{})
    if err != nil {
        log.Printf("Warning: could not load default PDB config, using built-in defaults: %v\n", err)
        return
    }
    updateDefaultPDBConfig(cm, clientset)
}

func runController(ctx context.Context, clientset *kubernetes.Clientset) {
    factory := informers.NewSharedInformerFactory(clientset, 10*time.Minute)

    deployInformer := factory.Apps().V1().Deployments().Informer()
    deployInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc:    func(obj interface{}) { createPDBForWorkload(ctx, clientset, obj) },
        UpdateFunc: func(old, new interface{}) { handleWorkloadUpdate(ctx, clientset, old, new) },
        DeleteFunc: func(obj interface{}) { deletePDBForWorkload(ctx, clientset, obj) },
    })

    stsInformer := factory.Apps().V1().StatefulSets().Informer()
    stsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc:    func(obj interface{}) { createPDBForWorkload(ctx, clientset, obj) },
        UpdateFunc: func(old, new interface{}) { handleWorkloadUpdate(ctx, clientset, old, new) },
        DeleteFunc: func(obj interface{}) { deletePDBForWorkload(ctx, clientset, obj) },
    })

    go func() {
        garbageCollectOrphanedPDBs(ctx, clientset)
        ticker := time.NewTicker(10 * time.Minute)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                garbageCollectOrphanedPDBs(ctx, clientset)
            case <-ctx.Done():
                return
            }
        }
    }()

    factory.Start(ctx.Done())
    for _, synced := range []cache.InformerSynced{deployInformer.HasSynced, stsInformer.HasSynced} {
        if !cache.WaitForCacheSync(ctx.Done(), synced) {
            panic("failed to sync informer cache")
        }
    }
    <-ctx.Done()
}

func handleWorkloadUpdate(ctx context.Context, clientset *kubernetes.Clientset, oldObj, newObj interface{}) {
    var oldAnnotations, newAnnotations map[string]string
    var namespace, name string

    switch oldWorkload := oldObj.(type) {
    case *appsv1.Deployment:
        oldAnnotations = oldWorkload.Annotations
        newWorkload := newObj.(*appsv1.Deployment)
        newAnnotations = newWorkload.Annotations
        namespace = newWorkload.Namespace
        name = newWorkload.Name
    case *appsv1.StatefulSet:
        oldAnnotations = oldWorkload.Annotations
        newWorkload := newObj.(*appsv1.StatefulSet)
        newAnnotations = newWorkload.Annotations
        namespace = newWorkload.Namespace
        name = newWorkload.Name
    default:
        return
    }

    oldBypass := false
    if oldAnnotations != nil {
        if val, ok := oldAnnotations[annotationBypass]; ok && val == "true" {
            oldBypass = true
        }
    }
    newBypass := false
    if newAnnotations != nil {
        if val, ok := newAnnotations[annotationBypass]; ok && val == "true" {
            newBypass = true
        }
    }

    // If bypass annotation is added, remove PDB
    if !oldBypass && newBypass {
        pdbName := fmt.Sprintf("castai-%s-pdb", name)
        err := clientset.PolicyV1().PodDisruptionBudgets(namespace).Delete(ctx, pdbName, metav1.DeleteOptions{})
        if err != nil && !apierrors.IsNotFound(err) {
            log.Printf("Failed to delete PDB %s/%s after bypass annotation added: %v\n", namespace, pdbName, err)
        } else {
            log.Printf("Bypass annotation added to %s/%s, removed PDB\n", namespace, name)
        }
        return
    }

    // If bypass annotation is removed, create PDB
    if oldBypass && !newBypass {
        log.Printf("Bypass annotation removed from %s/%s, creating PDB if needed\n", namespace, name)
        createPDBForWorkload(ctx, clientset, newObj)
        return
    }

    // If bypass annotation is present, do not create PDB
    if newBypass {
        return
    }

    // Check if minAvailable or maxUnavailable annotation changed
    oldMin := ""
    newMin := ""
    oldMax := ""
    newMax := ""
    if oldAnnotations != nil {
        oldMin = oldAnnotations[annotationMinAvailable]
        oldMax = oldAnnotations[annotationMaxUnavailable]
    }
    if newAnnotations != nil {
        newMin = newAnnotations[annotationMinAvailable]
        newMax = newAnnotations[annotationMaxUnavailable]
    }
    if oldMin != newMin || oldMax != newMax {
        log.Printf("PDB annotation changed for %s/%s, updating PDB\n", namespace, name)
        createPDBForWorkload(ctx, clientset, newObj)
        return
    }

    // For all other updates, run normal logic
    createPDBForWorkload(ctx, clientset, newObj)
}

func selectorMatchesLabels(selector, labels map[string]string) bool {
    if len(selector) == 0 {
        return false
    }
    for k, v := range selector {
        if labels[k] != v {
            return false
        }
    }
    return true
}

func parsePDBValue(input string) *intstr.IntOrString {
    input = strings.TrimSpace(input)
    if strings.HasSuffix(input, "%") {
        return &intstr.IntOrString{Type: intstr.String, StrVal: input}
    }
    if val, err := strconv.Atoi(input); err == nil {
        return &intstr.IntOrString{Type: intstr.Int, IntVal: int32(val)}
    }
    return nil
}

func logAndFixPoorPDBConfig(
    ctx context.Context,
    clientset *kubernetes.Clientset,
    pdb *policyv1.PodDisruptionBudget,
    workloadName string,
    replicas int32,
    namespace string,
    workloadObj interface{},
) {
    defaultPDBConfigLock.RLock()
    fixPoor := defaultPDBConfig.FixPoorPDBs
    defaultPDBConfigLock.RUnlock()

    poorConfig := false

    if pdb.Spec.MinAvailable != nil {
        min := pdb.Spec.MinAvailable
        if min.Type == intstr.Int && min.IntVal == replicas {
            key := fmt.Sprintf("%s/%s/minavailable-equals-replicas", namespace, pdb.Name)
            msg := fmt.Sprintf("WARNING: PDB %s/%s has minAvailable equal to replica count (%d). This is overly restrictive and may block disruptions.", namespace, pdb.Name, replicas)
            logPoorPDBWarning(key, msg)
            poorConfig = true
        }
        if min.Type == intstr.String && strings.TrimSpace(min.StrVal) == "100%" {
            key := fmt.Sprintf("%s/%s/minavailable-100-percent", namespace, pdb.Name)
            msg := fmt.Sprintf("WARNING: PDB %s/%s has minAvailable set to 100%%. This is overly restrictive and may block disruptions.", namespace, pdb.Name)
            logPoorPDBWarning(key, msg)
            poorConfig = true
        }
    }
    if pdb.Spec.MaxUnavailable != nil {
        max := pdb.Spec.MaxUnavailable
        if (max.Type == intstr.Int && max.IntVal == 0) {
            key := fmt.Sprintf("%s/%s/maxunavailable-zero", namespace, pdb.Name)
            msg := fmt.Sprintf("WARNING: PDB %s/%s has maxUnavailable set to 0. This is overly restrictive and may block disruptions.", namespace, pdb.Name)
            logPoorPDBWarning(key, msg)
            poorConfig = true
        }
        if (max.Type == intstr.String && strings.TrimSpace(max.StrVal) == "0%") {
            key := fmt.Sprintf("%s/%s/maxunavailable-zero-percent", namespace, pdb.Name)
            msg := fmt.Sprintf("WARNING: PDB %s/%s has maxUnavailable set to 0%%. This is overly restrictive and may block disruptions.", namespace, pdb.Name)
            logPoorPDBWarning(key, msg)
            poorConfig = true
        }
    }

    if poorConfig && fixPoor && workloadObj != nil {
        log.Printf("FixPoorPDBs enabled: Deleting poor PDB %s/%s and recreating with defaults.\n", namespace, pdb.Name)
        err := clientset.PolicyV1().PodDisruptionBudgets(namespace).Delete(ctx, pdb.Name, metav1.DeleteOptions{})
        if err != nil && !apierrors.IsNotFound(err) {
            log.Printf("Failed to delete poor PDB %s/%s: %v\n", namespace, pdb.Name, err)
            return
        }
        // Recreate PDB using the default creation flow for the workload
        createPDBForWorkload(ctx, clientset, workloadObj)
    }
}


func createPDBForWorkload(ctx context.Context, clientset *kubernetes.Clientset, obj interface{}) {
    var labels map[string]string
    var namespace, name string
    var workloadAnnotations map[string]string
    var replicas *int32

    switch workload := obj.(type) {
    case *appsv1.Deployment:
        replicas = workload.Spec.Replicas
        if replicas == nil || *replicas < 2 {
            return
        }
        if workload.Annotations != nil {
            if val, ok := workload.Annotations[annotationBypass]; ok && val == "true" {
                return
            }
        }
        labels = workload.Spec.Template.Labels
        workloadAnnotations = workload.Annotations
        namespace = workload.Namespace
        name = workload.Name
    case *appsv1.StatefulSet:
        replicas = workload.Spec.Replicas
        if replicas == nil || *replicas < 2 {
            return
        }
        if workload.Annotations != nil {
            if val, ok := workload.Annotations[annotationBypass]; ok && val == "true" {
                return
            }
        }
        labels = workload.Spec.Template.Labels
        workloadAnnotations = workload.Annotations
        namespace = workload.Namespace
        name = workload.Name
    default:
        return
    }

    if labels == nil {
        return
    }

    pdbList, err := clientset.PolicyV1().PodDisruptionBudgets(namespace).List(ctx, metav1.ListOptions{})
    if err != nil {
        log.Printf("Failed to list PDBs in namespace %s: %v\n", namespace, err)
        return
    }
    for _, pdb := range pdbList.Items {
        // Only match on exact matchLabels, not matchExpressions
        if pdb.Spec.Selector != nil && selectorMatchesLabels(pdb.Spec.Selector.MatchLabels, labels) {
            // Check if the PDB is poorly configured
            fixPoor := false
            defaultPDBConfigLock.RLock()
            fixPoor = defaultPDBConfig.FixPoorPDBs
            defaultPDBConfigLock.RUnlock()
            poorConfig := false
            if replicas != nil {
                poorConfig = isPoorPDBConfig(&pdb, *replicas)
            }
            if poorConfig && fixPoor && replicas != nil {
                logAndFixPoorPDBConfig(ctx, clientset, &pdb, name, *replicas, namespace, obj)
                // After fixing, do not return so the new PDB can be created
            } else {
                // Rate-limit "skipping PDB creation" logs
                key := fmt.Sprintf("%s/%s", namespace, name)
                now := time.Now()
                skipLogTimesLock.Lock()
                last, ok := skipLogTimes[key]
                if !ok || now.Sub(last) > skipLogInterval {
                    log.Printf("Skipping PDB creation for %s/%s: overlapping PDB %s exists\n", namespace, name, pdb.Name)
                    skipLogTimes[key] = now
                }
                skipLogTimesLock.Unlock()
                // Always return after handling/logging, to avoid duplicate PDBs
                return
            }
        }
    }

    pdbName := fmt.Sprintf("castai-%s-pdb", name)
    existingPDB, err := clientset.PolicyV1().PodDisruptionBudgets(namespace).Get(ctx, pdbName, metav1.GetOptions{})
    exists := err == nil
    if err != nil && !apierrors.IsNotFound(err) {
        log.Printf("Error checking for existing PDB %s/%s: %v\n", namespace, pdbName, err)
        return
    }

    var minAvailable *intstr.IntOrString
    var maxUnavailable *intstr.IntOrString

    if workloadAnnotations != nil {
        if val, ok := workloadAnnotations[annotationMinAvailable]; ok {
            minAvailable = parsePDBValue(val)
        }
        if val, ok := workloadAnnotations[annotationMaxUnavailable]; ok {
            maxUnavailable = parsePDBValue(val)
        }
    }

    if minAvailable == nil && maxUnavailable == nil {
        defaultPDBConfigLock.RLock()
        minAvailable = defaultPDBConfig.MinAvailable
        maxUnavailable = defaultPDBConfig.MaxUnavailable
        defaultPDBConfigLock.RUnlock()
    }

    if minAvailable != nil && maxUnavailable != nil {
        log.Printf("Invalid PDB spec for %s/%s: both pdb-minAvailable and pdb-maxUnavailable set\n", namespace, name)
        return
    }

    if minAvailable == nil && maxUnavailable == nil {
        minAvailable = &intstr.IntOrString{Type: intstr.Int, IntVal: 1}
    }

    pdbSpec := policyv1.PodDisruptionBudgetSpec{
        Selector: &metav1.LabelSelector{
            MatchLabels: labels,
        },
    }
    if minAvailable != nil {
        pdbSpec.MinAvailable = minAvailable
    }
    if maxUnavailable != nil {
        pdbSpec.MaxUnavailable = maxUnavailable
    }

    if exists {
        // Only update if spec has changed
        needsUpdate := false
        if existingPDB.Spec.MinAvailable == nil && pdbSpec.MinAvailable != nil {
            needsUpdate = true
        } else if existingPDB.Spec.MinAvailable != nil && pdbSpec.MinAvailable == nil {
            needsUpdate = true
        } else if existingPDB.Spec.MinAvailable != nil && pdbSpec.MinAvailable != nil &&
            existingPDB.Spec.MinAvailable.String() != pdbSpec.MinAvailable.String() {
            needsUpdate = true
        }
        if existingPDB.Spec.MaxUnavailable == nil && pdbSpec.MaxUnavailable != nil {
            needsUpdate = true
        } else if existingPDB.Spec.MaxUnavailable != nil && pdbSpec.MaxUnavailable == nil {
            needsUpdate = true
        } else if existingPDB.Spec.MaxUnavailable != nil && pdbSpec.MaxUnavailable != nil &&
            existingPDB.Spec.MaxUnavailable.String() != pdbSpec.MaxUnavailable.String() {
            needsUpdate = true
        }
        if needsUpdate {
            existingPDB.Spec = pdbSpec
            _, err := clientset.PolicyV1().PodDisruptionBudgets(namespace).Update(ctx, existingPDB, metav1.UpdateOptions{})
            if err != nil {
                log.Printf("Failed to update PDB for %s/%s: %v\n", namespace, name, err)
            } else {
                log.Printf("Updated PDB for %s/%s\n", namespace, name)
                if replicas != nil {
                    logAndFixPoorPDBConfig(ctx, clientset, existingPDB, name, *replicas, namespace, obj)
                }
            }
        }
        return
    }

    pdb := &policyv1.PodDisruptionBudget{
        ObjectMeta: metav1.ObjectMeta{
            Name:      pdbName,
            Namespace: namespace,
        },
        Spec: pdbSpec,
    }

    _, err = clientset.PolicyV1().PodDisruptionBudgets(namespace).Create(ctx, pdb, metav1.CreateOptions{})
    if err != nil {
        if apierrors.IsAlreadyExists(err) {
            return
        }
        log.Printf("Failed to create PDB for %s/%s: %v\n", namespace, name, err)
    } else {
        if replicas != nil {
            logAndFixPoorPDBConfig(ctx, clientset, pdb, name, *replicas, namespace, obj)
        }
    }
}


func deletePDBForWorkload(ctx context.Context, clientset *kubernetes.Clientset, obj interface{}) {
    var namespace, name string

    switch workload := obj.(type) {
    case *appsv1.Deployment:
        namespace = workload.Namespace
        name = workload.Name
    case *appsv1.StatefulSet:
        namespace = workload.Namespace
        name = workload.Name
    default:
        return
    }

    pdbName := fmt.Sprintf("castai-%s-pdb", name)
    err := clientset.PolicyV1().PodDisruptionBudgets(namespace).Delete(ctx, pdbName, metav1.DeleteOptions{})
    if err != nil && !apierrors.IsNotFound(err) {
        log.Printf("Failed to delete PDB %s/%s: %v\n", namespace, pdbName, err)
    }
}

func garbageCollectOrphanedPDBs(ctx context.Context, clientset *kubernetes.Clientset) {
    pdbs, err := clientset.PolicyV1().PodDisruptionBudgets("").List(ctx, metav1.ListOptions{})
    if err != nil {
        log.Printf("Failed to list PDBs: %v\n", err)
        return
    }
    for _, pdb := range pdbs.Items {
        if !strings.HasPrefix(pdb.Name, "castai-") || !strings.HasSuffix(pdb.Name, "-pdb") {
            continue
        }
        workloadName := strings.TrimSuffix(strings.TrimPrefix(pdb.Name, "castai-"), "-pdb")
        _, errDep := clientset.AppsV1().Deployments(pdb.Namespace).Get(ctx, workloadName, metav1.GetOptions{})
        _, errSts := clientset.AppsV1().StatefulSets(pdb.Namespace).Get(ctx, workloadName, metav1.GetOptions{})
        if apierrors.IsNotFound(errDep) && apierrors.IsNotFound(errSts) {
            err := clientset.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Delete(ctx, pdb.Name, metav1.DeleteOptions{})
            if err != nil && !apierrors.IsNotFound(err) {
                log.Printf("Failed to GC orphaned PDB %s/%s: %v\n", pdb.Namespace, pdb.Name, err)
            } else {
                log.Printf("Garbage collected orphaned PDB %s/%s\n", pdb.Namespace, pdb.Name)
            }
        }
    }
}

// Reconcile all workloads that use default PDB config
func reconcileAllDefaultPDBs(ctx context.Context, clientset *kubernetes.Clientset) {
    namespaces, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
    if err != nil {
        log.Printf("Failed to list namespaces: %v\n", err)
        return
    }
    for _, ns := range namespaces.Items {
        // Check Deployments
        deployments, err := clientset.AppsV1().Deployments(ns.Name).List(ctx, metav1.ListOptions{})
        if err == nil {
            for _, d := range deployments.Items {
                if !hasCustomPDBAnnotations(d.Annotations) && !hasBypassAnnotation(d.Annotations) && d.Spec.Replicas != nil && *d.Spec.Replicas >= 2 {
                    createPDBForWorkload(ctx, clientset, &d)
                }
            }
        }
        // Check StatefulSets
        statefulsets, err := clientset.AppsV1().StatefulSets(ns.Name).List(ctx, metav1.ListOptions{})
        if err == nil {
            for _, s := range statefulsets.Items {
                if !hasCustomPDBAnnotations(s.Annotations) && !hasBypassAnnotation(s.Annotations) && s.Spec.Replicas != nil && *s.Spec.Replicas >= 2 {
                    createPDBForWorkload(ctx, clientset, &s)
                }
            }
        }
    }
}

func hasCustomPDBAnnotations(annotations map[string]string) bool {
    if annotations == nil {
        return false
    }
    if _, ok := annotations[annotationMinAvailable]; ok {
        return true
    }
    if _, ok := annotations[annotationMaxUnavailable]; ok {
        return true
    }
    return false
}

func hasBypassAnnotation(annotations map[string]string) bool {
    if annotations == nil {
        return false
    }
    if val, ok := annotations[annotationBypass]; ok && val == "true" {
        return true
    }
    return false
}

// Scans all PDBs in the cluster for poor configuration at startup
func scanAllPDBsForPoorConfig(ctx context.Context, clientset *kubernetes.Clientset) {
    pdbs, err := clientset.PolicyV1().PodDisruptionBudgets("").List(ctx, metav1.ListOptions{})
    if err != nil {
        log.Printf("Failed to list PDBs for audit: %v\n", err)
        return
    }
    for _, pdb := range pdbs.Items {
        // Build a selector from the PDB spec
        if pdb.Spec.Selector == nil {
            continue
        }
        selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
        if err != nil {
            log.Printf("Invalid label selector for PDB %s/%s: %v\n", pdb.Namespace, pdb.Name, err)
            continue
        }

        // Find all matching Deployments
        deployments, err := clientset.AppsV1().Deployments(pdb.Namespace).List(ctx, metav1.ListOptions{})
        if err == nil {
            for _, deploy := range deployments.Items {
                if selector.Matches(labels.Set(deploy.Spec.Template.Labels)) {
                    replicas := int32(1)
                    if deploy.Spec.Replicas != nil {
                        replicas = *deploy.Spec.Replicas
                    }
                    logAndFixPoorPDBConfig(ctx, clientset, &pdb, deploy.Name, replicas, pdb.Namespace, &deploy)
                }
            }
        }

        // Find all matching StatefulSets
        statefulsets, err := clientset.AppsV1().StatefulSets(pdb.Namespace).List(ctx, metav1.ListOptions{})
        if err == nil {
            for _, sts := range statefulsets.Items {
                if selector.Matches(labels.Set(sts.Spec.Template.Labels)) {
                    replicas := int32(1)
                    if sts.Spec.Replicas != nil {
                        replicas = *sts.Spec.Replicas
                    }
                    logAndFixPoorPDBConfig(ctx, clientset, &pdb, sts.Name, replicas, pdb.Namespace, &sts)
                }
            }
        }
    }
}

func isPoorPDBConfig(pdb *policyv1.PodDisruptionBudget, replicas int32) bool {
    if pdb.Spec.MinAvailable != nil {
        min := pdb.Spec.MinAvailable
        if (min.Type == intstr.Int && min.IntVal == replicas) ||
           (min.Type == intstr.String && strings.TrimSpace(min.StrVal) == "100%") {
            return true
        }
    }
    if pdb.Spec.MaxUnavailable != nil {
        max := pdb.Spec.MaxUnavailable
        if (max.Type == intstr.Int && max.IntVal == 0) ||
           (max.Type == intstr.String && strings.TrimSpace(max.StrVal) == "0%") {
            return true
        }
    }
    return false
}
