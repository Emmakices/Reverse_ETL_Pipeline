CREATE EXTERNAL DATA SOURCE ds_revetl
WITH (
    LOCATION = 'https://strevpoc123.dfs.core.windows.net/strevpoc123',
    CREDENTIAL = synapse_managed_identity
);
