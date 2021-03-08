package controllers

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/spotinst/wave-operator/api/v1alpha1"
)

var _ = Describe("SparkPodController", func() {

	const (
		timeout  = time.Second * 10
		interval = time.Second
	)

	Context("When reconciling driver pod", func() {

		ctx := context.Background()
		sparkAppID := "my-spark-app-1"
		nsName := "test-ns"
		driverPodName := "my-spark-driver"
		executorPodName := "my-spark-executor"

		It("Should create a Spark application CR", func() {

			ns := &v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: nsName},
			}

			driverPod := getTestPod(nsName, driverPodName, "", DriverRole, sparkAppID, false)

			Expect(k8sClient.Create(ctx, ns)).Should(Succeed())
			Expect(k8sClient.Create(ctx, driverPod)).Should(Succeed())

			lookupKey := types.NamespacedName{Name: sparkAppID, Namespace: driverPod.Namespace}
			created := &v1alpha1.SparkApplication{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, lookupKey, created)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(created.Spec.ApplicationID).Should(Equal(sparkAppID))
			Expect(created.Spec.ApplicationName).Should(Equal(driverPod.Name))
			Expect(created.Spec.Heritage).Should(Equal(v1alpha1.SparkHeritageSubmit))
			Expect(created.Status.Data.Driver.Name).Should(Equal(driverPod.Name))
			Expect(created.Status.Data.Driver.Namespace).Should(Equal(driverPod.Namespace))

		})

		It("Should set finalizer on driver pod", func() {

			lookupKey := types.NamespacedName{Name: driverPodName, Namespace: nsName}
			updatedPod := &v1.Pod{}

			Expect(k8sClient.Get(ctx, lookupKey, updatedPod)).Should(Succeed())
			Expect(len(updatedPod.Finalizers)).Should(Equal(1))
			Expect(updatedPod.Finalizers[0]).Should(Equal(sparkApplicationFinalizerName))

		})

		It("Should set owner reference on driver pod", func() {

			lookupKey := types.NamespacedName{Name: driverPodName, Namespace: nsName}
			updatedPod := &v1.Pod{}

			Expect(k8sClient.Get(ctx, lookupKey, updatedPod)).Should(Succeed())
			Expect(len(updatedPod.OwnerReferences)).Should(Equal(1))
			Expect(updatedPod.OwnerReferences[0].APIVersion).Should(Equal(apiVersion))
			Expect(updatedPod.OwnerReferences[0].Kind).Should(Equal(sparkApplicationKind))
			Expect(updatedPod.OwnerReferences[0].Name).Should(Equal(sparkAppID))

		})

		It("Should add executors to CR as they come", func() {

			executorPod := getTestPod(nsName, executorPodName, "", ExecutorRole, sparkAppID, false)

			Expect(k8sClient.Create(ctx, executorPod)).Should(Succeed())

			lookupKey := types.NamespacedName{Name: sparkAppID, Namespace: nsName}
			updated := &v1alpha1.SparkApplication{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, lookupKey, updated)
				return err == nil && len(updated.Status.Data.Executors) == 1
			}, timeout, interval).Should(BeTrue())

			Expect(len(updated.Status.Data.Executors)).Should(Equal(1))
			Expect(updated.Status.Data.Executors[0].Name).Should(Equal(executorPod.Name))

		})

		It("Should set finalizer on executor pod", func() {

			lookupKey := types.NamespacedName{Name: executorPodName, Namespace: nsName}
			updatedPod := &v1.Pod{}

			Expect(k8sClient.Get(ctx, lookupKey, updatedPod)).Should(Succeed())
			Expect(len(updatedPod.Finalizers)).Should(Equal(1))
			Expect(updatedPod.Finalizers[0]).Should(Equal(sparkApplicationFinalizerName))

		})
	})

})
