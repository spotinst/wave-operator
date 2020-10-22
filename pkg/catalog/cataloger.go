package catalog

import (
	"github.com/spotinst/wave-operator/api/v1alpha1"
)

type Cataloger interface {

	// List returns the set of WaveComponents that are in the system
	List() (v1alpha1.WaveComponentList, error)

	// Get returns a WaveComponent by name
	Get(name string) (*v1alpha1.WaveComponent, error)

	// Update applies changes to the spec of a WaveComponent
    Update(component *v1alpha1.WaveComponent) error
}
