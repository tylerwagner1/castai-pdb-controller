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

    appsv1 "k8s.io/api/apps/v1"
    policyv1 "k8s.io/api/policy/v1"
    corev1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
)

type DefaultPDBConfig struct {
    MinAvailable   *intstr.IntOrString
    MaxUnavailable *intstr.IntOrString
}

var (
    defaultPDBConfig     DefaultPDBConfig
    defaultPDBConfigLock sync.RWMutex
)

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

    loadDefaultPDBConfig(ctx, clientset)
    go watchConfigMap(ctx, clientset)

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
                fmt.Printf("[%s] I am the leader now\n", id)
                runController(ctx, clientset)
            },
            OnStoppedLeading: func() {
                fmt.Printf("[%s] Lost leadership, exiting\n", id)
                os.Exit(0)
            },
            OnNewLeader: func(identity string) {
                if identity == id {
                    fmt.Printf("[%s] I am the new leader\n", id)
                } else {
                    fmt.Printf("[%s] New leader elected: %s\n", id, identity)
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
                updateDefaultPDBConfig(cm)
            }
        },
        UpdateFunc: func(oldObj, newObj interface{}) {
            if cm, ok := newObj.(*corev1.ConfigMap); ok && cm.Name == configMapName {
                updateDefaultPDBConfig(cm)
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

func updateDefaultPDBConfig(cm *corev1.ConfigMap) {
    var minAvailable, maxUnavailable *intstr.IntOrString
    if val, ok := cm.Data["minAvailable"]; ok {
        minAvailable = parsePDBValue(val)
    }
    if val, ok := cm.Data["maxUnavailable"]; ok {
        maxUnavailable = parsePDBValue(val)
    }
    if minAvailable != nil && maxUnavailable != nil {
        fmt.Printf("Invalid default PDB config: both minAvailable and maxUnavailable set in ConfigMap\n")
        minAvailable = nil
        maxUnavailable = nil
    }
    defaultPDBConfigLock.Lock()
    defaultPDBConfig.MinAvailable = minAvailable
    defaultPDBConfig.MaxUnavailable = maxUnavailable
    defaultPDBConfigLock.Unlock()
    fmt.Printf("Default PDB config updated from ConfigMap: minAvailable=%v, maxUnavailable=%v\n", minAvailable, maxUnavailable)
}

func resetDefaultPDBConfig() {
    defaultPDBConfigLock.Lock()
    defaultPDBConfig.MinAvailable = nil
    defaultPDBConfig.MaxUnavailable = nil
    defaultPDBConfigLock.Unlock()
    fmt.Printf("Default PDB config reset: using built-in fallback\n")
}

func loadDefaultPDBConfig(ctx context.Context, clientset *kubernetes.Clientset) {
    cm, err := clientset.CoreV1().ConfigMaps(configMapNamespace).Get(ctx, configMapName, metav1.GetOptions{})
    if err != nil {
        fmt.Printf("Warning: could not load default PDB config, using built-in defaults: %v\n", err)
        return
    }
    updateDefaultPDBConfig(cm)
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
            fmt.Printf("Failed to delete PDB %s/%s after bypass annotation added: %v\n", namespace, pdbName, err)
        } else {
            fmt.Printf("Bypass annotation added to %s/%s, removed PDB\n", namespace, name)
        }
        return
    }

    // If bypass annotation is removed, create PDB
    if oldBypass && !newBypass {
        fmt.Printf("Bypass annotation removed from %s/%s, creating PDB if needed\n", namespace, name)
        createPDBForWorkload(ctx, clientset, newObj)
        return
    }

    // If bypass annotation is present, do not create PDB
    if newBypass {
        return
    }

    // For all other updates, run normal logic
    createPDBForWorkload(ctx, clientset, newObj)
}

func anyLabelMatch(subset, set map[string]string) bool {
    for k, v := range subset {
        if set[k] == v {
            return true
        }
    }
    return false
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

func createPDBForWorkload(ctx context.Context, clientset *kubernetes.Clientset, obj interface{}) {
    var labels map[string]string
    var namespace, name string
    var workloadAnnotations map[string]string

    switch workload := obj.(type) {
    case *appsv1.Deployment:
        if workload.Spec.Replicas == nil || *workload.Spec.Replicas < 2 {
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
        if workload.Spec.Replicas == nil || *workload.Spec.Replicas < 2 {
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
        fmt.Printf("Failed to list PDBs in namespace %s: %v\n", namespace, err)
        return
    }
    for _, pdb := range pdbList.Items {
        if pdb.Spec.Selector != nil && anyLabelMatch(pdb.Spec.Selector.MatchLabels, labels) {
            fmt.Printf("Skipping PDB creation for %s/%s: overlapping PDB %s exists\n", namespace, name, pdb.Name)
            return
        }
    }

    pdbName := fmt.Sprintf("castai-%s-pdb", name)
    _, err = clientset.PolicyV1().PodDisruptionBudgets(namespace).Get(ctx, pdbName, metav1.GetOptions{})
    if err == nil {
        return
    }
    if !apierrors.IsNotFound(err) {
        fmt.Printf("Error checking for existing PDB %s/%s: %v\n", namespace, pdbName, err)
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
        fmt.Printf("Invalid PDB spec for %s/%s: both pdb-minAvailable and pdb-maxUnavailable set\n", namespace, name)
        return
    }

    if minAvailable == nil && maxUnavailable == nil {
        minAvailable = &intstr.IntOrString{Type: intstr.Int, IntVal: 1}
    }

    pdb := &policyv1.PodDisruptionBudget{
        ObjectMeta: metav1.ObjectMeta{
            Name:      pdbName,
            Namespace: namespace,
        },
        Spec: policyv1.PodDisruptionBudgetSpec{
            Selector: &metav1.LabelSelector{
                MatchLabels: labels,
            },
        },
    }

    if minAvailable != nil {
        pdb.Spec.MinAvailable = minAvailable
    }
    if maxUnavailable != nil {
        pdb.Spec.MaxUnavailable = maxUnavailable
    }

    _, err = clientset.PolicyV1().PodDisruptionBudgets(namespace).Create(ctx, pdb, metav1.CreateOptions{})
    if err != nil {
        if apierrors.IsAlreadyExists(err) {
            return
        }
        fmt.Printf("Failed to create PDB for %s/%s: %v\n", namespace, name, err)
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
        fmt.Printf("Failed to delete PDB %s/%s: %v\n", namespace, pdbName, err)
    }
}

func garbageCollectOrphanedPDBs(ctx context.Context, clientset *kubernetes.Clientset) {
    pdbs, err := clientset.PolicyV1().PodDisruptionBudgets("").List(ctx, metav1.ListOptions{})
    if err != nil {
        fmt.Printf("Failed to list PDBs: %v\n", err)
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
                fmt.Printf("Failed to GC orphaned PDB %s/%s: %v\n", pdb.Namespace, pdb.Name, err)
            } else {
                fmt.Printf("Garbage collected orphaned PDB %s/%s\n", pdb.Namespace, pdb.Name)
            }
        }
    }
}

