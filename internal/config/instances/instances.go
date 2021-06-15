//go:generate mockgen -destination=mock_instances/instances_mock.go . InstanceTypeManager

package instances

import (
	"fmt"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/robfig/cron/v3"

	"github.com/spotinst/wave-operator/internal/spot/client"
)

const (
	refreshInstanceTypesCronExpression = "@every 3m"
)

// InstanceTypes is a map of instance type family -> instance types
type instanceTypes struct {
	sync.RWMutex
	m map[string]map[string]bool
}

type manager struct {
	// allowedInstanceTypes is a cached map of allowed instance types
	allowedInstanceTypes instanceTypes
	clusterIdentifier    string
	oceanClient          client.OceanClient
	awsClient            client.AWSClient
	log                  logr.Logger
}

type InstanceTypeManager interface {
	Start() error
	ValidateInstanceType(instanceType string) error
	GetValidInstanceTypesInFamily(family string) ([]string, error)
}

func NewInstanceTypeManager(client *client.Client, clusterIdentifier string, log logr.Logger) InstanceTypeManager {
	return &manager{
		allowedInstanceTypes: instanceTypes{m: make(map[string]map[string]bool)},
		clusterIdentifier:    clusterIdentifier,
		oceanClient:          client,
		awsClient:            client,
		log:                  log,
	}
}

func (m *manager) Start() error {
	// Initial refresh
	if err := m.refreshAllowedInstanceTypes(); err != nil {
		return fmt.Errorf("could not refresh instance types, %w", err)
	}

	// Schedule ongoing refreshes
	refreshFunc := func() {
		if err := m.refreshAllowedInstanceTypes(); err != nil {
			m.log.Error(err, "could not refresh instance types")
		}
	}
	c := cron.New(cron.WithChain(cron.Recover(m.log)))
	_, err := c.AddFunc(refreshInstanceTypesCronExpression, refreshFunc)
	if err != nil {
		return fmt.Errorf("could not schedule instance types refresh, %w", err)
	}
	m.log.Info(fmt.Sprintf("Scheduling instance type refreshes %s", refreshInstanceTypesCronExpression))
	c.Start()

	return nil
}

func (m *manager) ValidateInstanceType(instanceType string) error {
	m.allowedInstanceTypes.RLock()
	defer m.allowedInstanceTypes.RUnlock()
	if len(m.allowedInstanceTypes.m) > 0 {
		// Validate instance type is allowed
		family, err := getFamily(instanceType)
		if err != nil {
			return fmt.Errorf("could not get family for instance type %q, %w", instanceType, err)
		}
		if m.allowedInstanceTypes.m[family][instanceType] == false {
			return fmt.Errorf("instance type %q not allowed", instanceType)
		}
	} else {
		// Just validate string format
		if !validateInstanceTypeFormat(instanceType) {
			return fmt.Errorf("malformed instance type %q", instanceType)
		}
	}
	return nil
}

func (m *manager) GetValidInstanceTypesInFamily(family string) ([]string, error) {
	m.allowedInstanceTypes.RLock()
	defer m.allowedInstanceTypes.RUnlock()
	instanceTypesInFamily := m.allowedInstanceTypes.m[family]
	if len(instanceTypesInFamily) == 0 {
		return nil, fmt.Errorf("instance type family %q not allowed", family)
	}
	allowedInstanceTypesInFamily := make([]string, len(instanceTypesInFamily))
	i := 0
	for k := range instanceTypesInFamily {
		allowedInstanceTypesInFamily[i] = k
		i++
	}
	return allowedInstanceTypesInFamily, nil
}

func (m *manager) refreshAllowedInstanceTypes() error {
	m.log.Info("Refreshing allowed instance types ...")
	allowed, err := m.fetchAllowedInstanceTypes()
	if err != nil {
		return err
	}
	m.allowedInstanceTypes.Lock()
	defer m.allowedInstanceTypes.Unlock()
	m.allowedInstanceTypes.m = allowed
	m.log.Info("Successfully refreshed allowed instance types")
	return nil
}

func (m *manager) fetchAllowedInstanceTypes() (map[string]map[string]bool, error) {

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

	var whitelistIgnoredInstanceTypes []string
	var blacklistIgnoredInstanceTypes []string

	for _, it := range instanceTypesInRegion {
		family, err := getFamily(it.InstanceType)
		if err != nil {
			m.log.Info(fmt.Sprintf("Could not get family for instance type %q, ignoring", it.InstanceType))
		} else {
			if whitelist != nil && whitelist[it.InstanceType] == false {
				// If instance type not present in whitelist, we don't want it
				whitelistIgnoredInstanceTypes = append(whitelistIgnoredInstanceTypes, it.InstanceType)
			} else if blacklist != nil && blacklist[it.InstanceType] == true {
				// If instance type present in blacklist, we don't want it
				blacklistIgnoredInstanceTypes = append(blacklistIgnoredInstanceTypes, it.InstanceType)
			} else {
				// This is an allowed instance type
				if allowed[family] == nil {
					allowed[family] = make(map[string]bool)
				}
				allowed[family][it.InstanceType] = true
			}
		}
	}

	if len(whitelistIgnoredInstanceTypes) > 0 {
		m.log.Info(fmt.Sprintf("Ignored %d instance types not in whitelist", len(whitelistIgnoredInstanceTypes)))
	}
	if len(blacklistIgnoredInstanceTypes) > 0 {
		m.log.Info(fmt.Sprintf("Ignored %d instance types in blacklist", len(blacklistIgnoredInstanceTypes)))
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
