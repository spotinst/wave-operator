/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ComponentType string
type ComponentStatus string

const (
	HelmComponentType      ComponentType   = "helm"
	PresentComponentStatus ComponentStatus = "present"
	AbsentComponentStatus  ComponentStatus = "absent"
)

// WaveComponentSpec defines the desired state of WaveComponent
type WaveComponentSpec struct {

	//Type is one of ["helm",]
	Type ComponentType `json:"type"`

	//Name is the name of a helm chart
	Name string `json:"name"`

	//Status determines whether the component should be installed or removed
	Status ComponentStatus `json:"status"`

	//URL is the location of the helm repository
	URL string `json:"url"`

	//Version is the version of the helm chart
	Version string `json:"version"`

	ValuesConfiguration string `json:"valueConfig,omitempty"`
}

// WaveComponentStatus defines the observed state of WaveComponent
type WaveComponentStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +kubebuilder:object:root=true

// WaveComponent is the Schema for the wavecomponents API
type WaveComponent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WaveComponentSpec   `json:"spec,omitempty"`
	Status WaveComponentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WaveComponentList contains a list of WaveComponent
type WaveComponentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WaveComponent `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WaveComponent{}, &WaveComponentList{})
}
