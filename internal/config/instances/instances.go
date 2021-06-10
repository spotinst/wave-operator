package instances

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"

	"github.com/spotinst/wave-operator/internal/spot/client"
)

// instanceTypes is a map of instance type family -> instance types
type instanceTypes map[string]map[string]bool

// allowedInstanceTypes is a cached map of allowed instance types
var allowedInstanceTypes instanceTypes

type manager struct {
	clusterIdentifier string
	oceanClient       client.OceanClient
	awsClient         client.AWSClient
	log               logr.Logger
}

type InstanceTypeManager interface {
	Start() error
}

func NewInstanceTypeManager(client *client.Client, clusterIdentifier string, log logr.Logger) InstanceTypeManager {
	return &manager{
		clusterIdentifier: clusterIdentifier,
		oceanClient:       client,
		awsClient:         client,
		log:               log,
	}
}

func (m *manager) Start() error {
	// TODO cron
	return m.refreshAllowedInstanceTypes()
}

func (m *manager) refreshAllowedInstanceTypes() error {
	m.log.Info("Refreshing allowed instance types ...")
	allowed, err := m.fetchAllowedInstanceTypes()
	if err != nil {
		return err
	}
	allowedInstanceTypes = allowed
	m.log.Info("Successfully refreshed allowed instance types")
	return nil
}

func (m *manager) fetchAllowedInstanceTypes() (instanceTypes, error) {

	// TODO(thorsteinn) We should call a backend endpoint to get this information

	allowed := make(map[string]map[string]bool)

	oceanCluster, err := getOceanCluster(m.oceanClient, m.clusterIdentifier)
	if err != nil {
		return nil, fmt.Errorf("could not get ocean cluster, %w", err)
	}

	if oceanCluster == nil {
		return nil, fmt.Errorf("ocean cluster is nil")
	}

	instanceTypesInRegion, err := getInstanceTypesInRegion(m.awsClient, oceanCluster.Region)
	if err != nil {
		return nil, fmt.Errorf("could not get instance types in region, %w", err)
	}

	whitelist := listToMap(oceanCluster.Compute.InstanceTypes.Whitelist)
	blacklist := listToMap(oceanCluster.Compute.InstanceTypes.Blacklist)

	for _, it := range instanceTypesInRegion {
		family, err := getFamily(it.InstanceType)
		if err != nil {
			m.log.Info(fmt.Sprintf("Could not get family for instance type %q, ignoring", it.InstanceType))
		} else {
			if whitelist != nil && whitelist[it.InstanceType] == false {
				// If instance type not present in whitelist, we don't want it
				m.log.Info(fmt.Sprintf("Instance type %q not present in whitelist, ignoring", it.InstanceType))
			} else if blacklist != nil && blacklist[it.InstanceType] == true {
				// If instance type present in blacklist, we don't want it
				m.log.Info(fmt.Sprintf("Instance type %q present in blacklist, ignoring", it.InstanceType))
			} else {
				// This is an allowed instance type
				if allowed[family] == nil {
					allowed[family] = make(map[string]bool)
				}
				allowed[family][it.InstanceType] = true
			}
		}
	}

	return allowed, nil
}

func listToMap(list []string) map[string]bool {
	if list == nil {
		return nil
	}
	m := make(map[string]bool)
	for _, item := range list {
		m[item] = true
	}
	return m
}

func getFamily(instanceType string) (string, error) {
	if !validateInstanceTypeFormat(instanceType) {
		return "", fmt.Errorf("malformed instance type %q", instanceType)
	}
	return strings.Split(instanceType, ".")[0], nil
}

// validateInstanceType validates that the given string is of the form family.type (e.g. m5.large)
func validateInstanceTypeFormat(instanceType string) bool {
	split := strings.Split(instanceType, ".")
	if len(split) != 2 {
		return false
	}
	for _, s := range split {
		if len(s) == 0 {
			return false
		}
	}
	return true
}

func getOceanCluster(clusterGetter client.OceanClusterGetter, clusterIdentifier string) (*client.OceanCluster, error) {
	oceanClusters, err := clusterGetter.GetAllOceanClusters()
	if err != nil {
		return nil, fmt.Errorf("could not get ocean clusters, %w", err)
	}

	foundOceanClusters := make([]*client.OceanCluster, 0)
	for i, oc := range oceanClusters {
		if oc.ControllerClusterId == clusterIdentifier {
			foundOceanClusters = append(foundOceanClusters, oceanClusters[i])
		}
	}

	if len(foundOceanClusters) != 1 {
		return nil, fmt.Errorf("found %d ocean clusters with controllerClusterId %q", len(foundOceanClusters), clusterIdentifier)
	}

	return foundOceanClusters[0], nil
}

func getInstanceTypesInRegion(instanceTypeGetter client.InstanceTypesGetter, region string) ([]*client.InstanceType, error) {
	return instanceTypeGetter.GetAvailableInstanceTypesInRegion(region)
}

// ValidateAndExpandFamily processes the given string and returns a list of strings
// If the given string is a valid instance type (e.g. m5.large), it returns a list with only one element
// If the given string is a valid instance family (e.g. m5), it returns a list of all allowed instance types within that family
// If the given string is invalid, it throws an error
func ValidateAndExpandFamily(input string) ([]string, error) {
	// If we don't have a cached list of allowed instance types, just validate string format
	if len(allowedInstanceTypes) == 0 {
		if validateInstanceTypeFormat(input) {
			return []string{input}, nil
		} else {
			return nil, fmt.Errorf("invalid instance type %q", input)
		}
	}

	if validateInstanceTypeFormat(input) {
		// This is a valid instance type name (family.instancetype)
		family, err := getFamily(input)
		if err != nil {
			return nil, fmt.Errorf("could not get instance family from %q", input)
		}
		if allowedInstanceTypes[family][input] == true {
			return []string{input}, nil
		}
		return nil, fmt.Errorf("instance type %q not allowed", input)
	} else {
		// Is this a valid instance type family?
		instanceTypesInFamily := allowedInstanceTypes[input]
		if len(instanceTypesInFamily) == 0 {
			return nil, fmt.Errorf("instance type family %q not allowed", input)
		}
		allowedInstanceTypesInFamily := make([]string, len(instanceTypesInFamily))
		i := 0
		for k, _ := range instanceTypesInFamily {
			allowedInstanceTypesInFamily[i] = k
			i++
		}
		return allowedInstanceTypesInFamily, nil
	}
}
