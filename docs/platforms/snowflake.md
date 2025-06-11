# Snowflake Assets

Bruin supports Snowflake as a data platform. 

## Connection
In order to set up a Snowflake connection, you need to add a configuration item to `connections` in the `.bruin.yml` file.

There's 2 different ways to fill it in

```yaml
    connections:
      snowflake:
        - name: "connection_name"
          username: "sfuser"
          password: "XXXXXXXXXX"
          account: "AAAAAAA-AA00000"
          database: "dev"
          schema: "schema_name" # optional
          warehouse: "warehouse_name" # optional
          role: "data_analyst" # optional
          region: "eu-west1" # optional
          private_key_path: "path/to/private_key" # optional
```

Where account is the identifier that you can copy here:

![Snowflake Account](/snowflake.png)


### Key-based Authentication

Snowflake currently supports both password-based authentication as well as key-based authentication. In order to use key-based authentication, you need to provide a path to the private key file as the `private_key_path` parameter. See [this guide](https://select.dev/docs/snowflake-developer-guide/snowflake-key-pair) to create a key-pair if you haven't done that before.


## Snowflake Assets

### `sf.sql`
Runs a materialized Snowflake asset or a Snowflake script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.


#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: events.install
type: sf.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a Snowflake script
```bruin-sql
/* @bruin
name: events.install
type: sf.sql
@bruin */

create temp table first_installs as
select 
    user_id, 
    min(ts) as install_ts,
    min_by(platform, ts) as platform,
    min_by(country, ts) as country
from analytics.events
where event_name = "install"
group by 1;

create or replace table events.install
select
    user_id, 
    i.install_ts,
    i.platform, 
    i.country,
    a.channel,
from first_installs as i
join marketing.attribution as a
    using(user_id)
```

### `sf.sensor.query`


Checks if a query returns any results in Snowflake, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters:**
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table
Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: sf.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table
Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: sf.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `sf.seed`
`sf.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Snowflake database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Snowflake database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: sf.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Snowflake database

The examples below show how to load a CSV into a Snowflake database.
```yaml
name: dashboard.hello
type: sf.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

### Key-based Authentication

Snowflake currently supports both password-based authentication as well as key-based authentication. In order to use key-based 
authentication, follow the following steps. 

# Step-by-Step: Snowflake Key Pair Authentication

## Step 1: Generate a Key Pair

Open your terminal and run the following command to create a key pair. If you’re using a mac, openssl should be installed 
by default so no additional setup is required.

```bash
openssl genrsa -out rsa_key.pem 2048
```

```bash
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub
```

## 2. Extract Public Key in the Correct Format

To use with Snowflake, extract the public key in base64 DER format:

```bash
openssl rsa -in rsa_key.pem -pubout -outform DER | openssl base64 -A
```

### 3. Set Public Key on Snowflake User

Log into Snowflake as an admin, create a new worksheet and run the following command :

```sql
ALTER USER your_snowflake_username
SET RSA_PUBLIC_KEY='your_base64_public_key_here';
```

For example:

```sql
ALTER USER JOHN_DOE
SET RSA_PUBLIC_KEY='MIITIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3nD038XL7myESbMhTxzN
af1dUU2jkfeJby5/l5rQ6gy5uivtdgysdACKuMWmahhmdT1x77lbHa6LxlIebeVN
EJhOCg8wH0c2v+TvAcv9RQgvRy1Er2dz188ETExPhUfq7CJ3AqJ6RYiREPiE1iTE
ikkTXt0Vq8m3tP/DbO45g5v43ZWvPyQ+MhvCN3mECRTwzE6uueA+QiK4K5jOn1R1
CWsKDzzLUd3IvGnBoqoigOq5zQJfQN7ZaIOTYakab48oXi4iFZKBrhhGDw0Cnl+S
olfm4uu+DshVbiR9gY4Xmh/KOX9fpLjUH6wCVfOgtOa3/gkFDJzrfwb+v8R27HJx
rQIDAQAB';
```

### 4. Verify
```sql
DESC USER your_snowflake_username;
```

This will show a column named RSA_PUBLIC_KEY, and you should see your Base64 key there.

Optionally if you want to verify that the key is set correctly, run:

```bash
openssl rsa -pubin -in rsa_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64
```

The output should match the value in the RSA_PUBLIC_KEY_FP column in Snowflake in the same table where you just inspected
RSA_PUBLIC_KEY.

### 5. Update Bruin Configuration

In your `.bruin.yml` file, update the Snowflake connection configuration to include the `private_key_path` parameter pointing 
to your private key file. For example:

```yaml
            snowflake:
                - name: snowflake-default
                  username: JOHN_DOE
                  account: EXAMPLE-ACCOUNT
                  database: dev
                  private_key_path: /Users/johndoe/rsa_key.pem
```


For more details on how to set up key-based authentication, see [this guide](https://select.dev/docs/snowflake-developer-guide/snowflake-key-pair).
