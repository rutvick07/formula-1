# Databricks notebook source
# MAGIC %md
# MAGIC #### Mounting & Unmounting the Azure Data Lake Storage 'f1datastoragelake' to 'dbfs'.

# COMMAND ----------

# variables declaration
client_id = dbutils.secrets.get('formula1-secretscope','formula1-spclientid')
tenant_id = dbutils.secrets.get('formula1-secretscope','formula1-tenantid')
client_secret = dbutils.secrets.get('formula1-secretscope','formula1-spclient-secret')

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

# A common function that taks 'container_name' to mount ADLS 'f1datastoragelake' containers.
# Python function to mount the list of containers under '/mnt/f1datastoragelake/'
def mountFormulaOneADLSStorage(mountList):
    for mount in mountList:
        dbutils.fs.mount(source = f"abfss://{mount}@f1datastoragelake.dfs.core.windows.net/",
                        mount_point = f"/mnt/f1datastoragelake/{mount}",
                        extra_configs = configs)
mountFormulaOneADLSStorage(['raw','processed','presentation','demo'])

# COMMAND ----------

# Python function to unmount all the mounted containers under '/mnt/f1datastoragelake/'
def unMountFormulaOneADLSStorage(unMountList):
    for mount in unMountList:
        dbutils.fs.unmount(f'dbfs:/mnt/f1datastoragelake/{mount}')

unMountFormulaOneADLSStorage(['raw','processed','presentation','demo'])
