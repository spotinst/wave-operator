//go:generate mockgen -destination=mock_instances/instances_mock.go . InstanceTypeManager

package instances

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/internal/spot/client"
)

const (
	refreshInstanceTypesInterval = 3 * time.Minute
)

type InstanceType struct {
	Family InstanceTypeFamily
	Type   string
}

type InstanceTypeFamily string

// InstanceTypes is a map of instance type family -> instance types
type instanceTypes struct {
	sync.RWMutex
	m map[InstanceTypeFamily]map[InstanceType]bool
}

type manager struct {
	// allowedInstanceTypes is a cached map of allowed instance types
	allowedInstanceTypes instanceTypes
	stopRefreshChan      chan bool
	clusterIdentifier    string
	oceanClient          client.OceanClient
	awsClient            client.AWSClient
	log                  logr.Logger
}

type InstanceTypeManager interface {
	Start() error
	Stop()
	ValidateInstanceType(instanceType string) error
	GetValidInstanceTypesInFamily(family string) ([]string, error)
}

func NewInstanceTypeManager(client *client.Client, clusterIdentifier string, log logr.Logger) InstanceTypeManager {
	return &manager{
		allowedInstanceTypes: instanceTypes{m: make(map[InstanceTypeFamily]map[InstanceType]bool)},
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
	m.log.Info(fmt.Sprintf("Scheduling instance type refreshes every %s", refreshInstanceTypesInterval))
	ticker := time.NewTicker(refreshInstanceTypesInterval)
	quit := make(chan bool)
	go func() {
		for {
			select {
			case <-ticker.C:
				if err := m.refreshAllowedInstanceTypes(); err != nil {
					m.log.Error(err, "could not refresh instance types")
				}
			case <-quit:
				m.log.Info("Stopping instance type refreshes")
				ticker.Stop()
				return
			}
		}
	}()
	m.stopRefreshChan = quit

	return nil
}

func (m *manager) Stop() {
	if m.stopRefreshChan != nil {
		close(m.stopRefreshChan)
	}
}

func (m *manager) ValidateInstanceType(instanceType string) error {
	parsed, err := instanceTypeFromString(instanceType)
	if err != nil {
		return fmt.Errorf("could not parse instance type %q, %w", instanceType, err)
	}
	m.allowedInstanceTypes.RLock()
	defer m.allowedInstanceTypes.RUnlock()
	if len(m.allowedInstanceTypes.m) > 0 {
		// Validate instance type is allowed
		if m.allowedInstanceTypes.m[parsed.Family][parsed] == false {
			return fmt.Errorf("instance type %q not allowed", instanceType)
		}
	}
	return nil
}

func (m *manager) GetValidInstanceTypesInFamily(family string) ([]string, error) {
	m.allowedInstanceTypes.RLock()
	defer m.allowedInstanceTypes.RUnlock()
	instanceTypesInFamily := m.allowedInstanceTypes.m[InstanceTypeFamily(family)]
	if len(instanceTypesInFamily) == 0 {
		return nil, fmt.Errorf("instance type family %q not allowed", family)
	}
	allowedInstanceTypesInFamily := make([]string, len(instanceTypesInFamily))
	i := 0
	for k := range instanceTypesInFamily {
		allowedInstanceTypesInFamily[i] = k.String()
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

func (m *manager) fetchAllowedInstanceTypes() (map[InstanceTypeFamily]map[InstanceType]bool, error) {

	// TODO(thorsteinn) We should call a backend endpoint to get this information pre-baked

	allowed := make(map[InstanceTypeFamily]map[InstanceType]bool)

	oceanCluster, err := getOceanCluster(m.oceanClient, m.clusterIdentifier)
	if err != nil {
		return nil, fmt.Errorf("could not get ocean cluster, %w", err)
	}

	if oceanCluster == nil {
		return nil, fmt.Errorf("ocean cluster is nil")
	}

	instanceTypesInRegion, err := getInstanceTypesInRegion(m.awsClient, oceanCluster.Region, m.log)
	if err != nil {
		return nil, fmt.Errorf("could not get instance types in region, %w", err)
	}

	whitelist := listToMap(oceanCluster.Compute.InstanceTypes.Whitelist)
	blacklist := listToMap(oceanCluster.Compute.InstanceTypes.Blacklist)

	var whitelistIgnoredInstanceTypes []string
	var blacklistIgnoredInstanceTypes []string

	for _, it := range instanceTypesInRegion {
		if whitelist != nil && whitelist[it.String()] == false {
			// If instance type not present in whitelist, we don't want it
			whitelistIgnoredInstanceTypes = append(whitelistIgnoredInstanceTypes, it.String())
		} else if blacklist != nil && blacklist[it.String()] == true {
			// If instance type present in blacklist, we don't want it
			blacklistIgnoredInstanceTypes = append(blacklistIgnoredInstanceTypes, it.String())
		} else {
			// This is an allowed instance type
			if allowed[it.Family] == nil {
				allowed[it.Family] = make(map[InstanceType]bool)
			}
			allowed[it.Family][it] = true
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

func getInstanceTypesInRegion(instanceTypeGetter client.InstanceTypesGetter, region string, log logr.Logger) ([]InstanceType, error) {
	its, err := instanceTypeGetter.GetAvailableInstanceTypesInRegion(region)
	if err != nil {
		return nil, fmt.Errorf("could not get available instance types in region, %w", err)
	}

	var instanceTypes []InstanceType
	for _, it := range its {
		instanceType, err := instanceTypeFromString(it.InstanceType)
		if err != nil {
			log.Info(fmt.Sprintf("Could not parse instance type %q, ignoring", it.InstanceType))
		} else {
			instanceTypes = append(instanceTypes, instanceType)
		}
	}

	return instanceTypes, nil
}

func (it InstanceType) String() string {
	return fmt.Sprintf("%s.%s", it.Family, it.Type)
}

func instanceTypeFromString(it string) (InstanceType, error) {
	split := strings.Split(it, ".")
	if len(split) != 2 {
		return InstanceType{}, fmt.Errorf("malformed instance type %q", it)
	}
	if len(split[0]) == 0 || len(split[1]) == 0 {
		return InstanceType{}, fmt.Errorf("malformed instance type %q", it)
	}
	return InstanceType{
		Family: InstanceTypeFamily(split[0]),
		Type:   split[1],
	}, nil
}
