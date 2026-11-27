targetScope = 'resourceGroup'

@description('Azure deployment region. UAE North is the selected target; validate service availability before deployment.')
param location string = 'uaenorth'

@description('Short globally unique suffix used in resource names.')
@minLength(4)
@maxLength(10)
param nameSuffix string

@description('Deploy ADLS Gen2 storage. Disabled by default to prevent accidental spend.')
param deployStorage bool = false

@description('Deploy Event Hubs Standard and a Kafka-compatible event hub. Disabled by default to prevent accidental spend.')
param deployEventHubs bool = false

@description('Deploy Log Analytics workspace. Disabled by default.')
param deployMonitoring bool = false

@description('Explicitly allow public endpoints. Keep false for secure deployments; enable only for isolated development experiments.')
param allowPublicNetworkAccess bool = false

@description('Resource tags.')
param tags object = {
  project: 'Egypt-Air-Quality-Intelligence-Platform'
  environment: 'non-production'
  managedBy: 'bicep'
  dataClassification: 'telemetry'
}

var storageName = take('eaqi${nameSuffix}adls', 24)
var eventHubNamespaceName = take('eaqi-${nameSuffix}-eh', 50)
var workspaceName = take('eaqi-${nameSuffix}-law', 63)

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = if (deployStorage) {
  name: storageName
  location: location
  tags: tags
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    publicNetworkAccess: allowPublicNetworkAccess ? 'Enabled' : 'Disabled'
    networkAcls: {
      bypass: allowPublicNetworkAccess ? 'AzureServices' : 'None'
      defaultAction: allowPublicNetworkAccess ? 'Allow' : 'Deny'
    }
  }
}

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = if (deployEventHubs) {
  name: eventHubNamespaceName
  location: location
  tags: tags
  sku: { name: 'Standard', tier: 'Standard', capacity: 1 }
  properties: {
    isAutoInflateEnabled: false
    maximumThroughputUnits: 0
    publicNetworkAccess: allowPublicNetworkAccess ? 'Enabled' : 'Disabled'
  }
}

resource telemetryHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = if (deployEventHubs) {
  parent: eventHubNamespace
  name: 'telemetry'
  properties: { messageRetentionInDays: 1, partitionCount: 2, status: 'Active' }
}

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = if (deployMonitoring) {
  name: workspaceName
  location: location
  tags: tags
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
    publicNetworkAccessForIngestion: allowPublicNetworkAccess ? 'Enabled' : 'Disabled'
    publicNetworkAccessForQuery: allowPublicNetworkAccess ? 'Enabled' : 'Disabled'
  }
}

output storageAccountId string = deployStorage ? storage.id : ''
output eventHubNamespaceId string = deployEventHubs ? eventHubNamespace.id : ''
output eventHubName string = deployEventHubs ? telemetryHub.name : ''
output logAnalyticsWorkspaceId string = deployMonitoring ? workspace.id : ''
