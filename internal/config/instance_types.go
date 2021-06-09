package config

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"

	"github.com/spotinst/wave-operator/internal/spot/client"
)

// InstanceTypes is a map of instance type family -> instance types
type InstanceTypes map[string]map[string]bool

type manager struct {
	allowedInstanceTypes InstanceTypes
	clusterIdentifier    string
	oceanClient          client.OceanClient
	awsClient            client.AWSClient
	log                  logr.Logger
}

type InstanceTypeManager interface {
	GetAllowedInstanceTypes() (InstanceTypes, error)
}

func NewInstanceTypeManager(client *client.Client, clusterIdentifier string, log logr.Logger) InstanceTypeManager {
	return &manager{
		clusterIdentifier: clusterIdentifier,
		oceanClient:       client,
		awsClient:         client,
		log:               log,
	}
}

func (m *manager) GetAllowedInstanceTypes() (InstanceTypes, error) {
	if m.allowedInstanceTypes == nil || len(m.allowedInstanceTypes) == 0 {
		m.log.Info("Allowed instance types not loaded, fetching now ...")
		if err := m.loadAllowedInstanceTypes(); err != nil {
			return InstanceTypes{}, fmt.Errorf("could not load allowed instance types, %w", err)
		}
	}
	return m.allowedInstanceTypes, nil
}

func (m *manager) loadAllowedInstanceTypes() error {

	// TODO(thorsteinn) We should call a backend endpoint to get this information

	oceanCluster, err := getOceanCluster(m.oceanClient, m.clusterIdentifier)
	if err != nil {
		return fmt.Errorf("could not get ocean cluster, %w", err)
	}

	if oceanCluster == nil {
		return fmt.Errorf("ocean cluster is nil")
	}

	instanceTypesInRegion, err := getInstanceTypesInRegion(m.awsClient, oceanCluster.Region)
	if err != nil {
		return fmt.Errorf("could not get instance types in region, %w", err)
	}

	m.allowedInstanceTypes = make(map[string]map[string]bool)

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
				if m.allowedInstanceTypes[family] == nil {
					m.allowedInstanceTypes[family] = make(map[string]bool)
				}
				m.allowedInstanceTypes[family][it.InstanceType] = true
			}
		}
	}

	return nil
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
	// Instance types should be of the form family.type (e.g. m5.xlarge)
	split := strings.Split(instanceType, ".")
	if len(split) != 2 {
		return "", fmt.Errorf("malformed instance type %q", instanceType)
	}
	for _, s := range split {
		if len(s) == 0 {
			return "", fmt.Errorf("malformed instance type %q", instanceType)
		}
	}
	return split[0], nil
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
