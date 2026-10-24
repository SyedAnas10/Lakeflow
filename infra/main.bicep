// Azure resources for Lakeflow: an ADLS Gen2 storage account with lifecycle rules and a
// Service Bus queue. Example only; not deployed from this repo.
// az deployment group create -g <rg> -f infra/main.bicep -p prefix=lakeflowdev

param prefix string
param location string = resourceGroup().location

resource lake 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: '${prefix}lake'
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true            // hierarchical namespace = ADLS Gen2
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false   // force Entra ID / managed identity, no account keys
  }
}

resource blob 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: lake
  name: 'default'
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blob
  name: 'lake'
}

resource lifecycle 'Microsoft.Storage/storageAccounts/managementPolicies@2023-01-01' = {
  parent: lake
  name: 'default'
  properties: {
    policy: loadJsonContent('lifecycle-policy.json')
  }
}

resource bus 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' = {
  name: '${prefix}-bus'
  location: location
  sku: { name: 'Standard', tier: 'Standard' }  // Standard is needed for duplicate detection
  properties: { disableLocalAuth: true }       // managed identity only
}

resource queue 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = {
  parent: bus
  name: 'ingest-jobs'
  properties: {
    lockDuration: 'PT1M'
    maxDeliveryCount: 5
    requiresDuplicateDetection: true
    duplicateDetectionHistoryTimeWindow: 'PT10M'
    deadLetteringOnMessageExpiration: true
  }
}

output storageAccount string = lake.name
output serviceBusNamespace string = bus.name
