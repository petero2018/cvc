# CVC  
Take-Home Exercise

---

# Local Setup Guide

## Step 1 – Install Docker
Install Docker if it's not yet available on your computer:  
👉 [Get Started with Docker](https://www.docker.com/get-started/)

---

## Step 2 – Configure dbt Profile 

Under the following location `\mini_data_warehouse\local_dbt_profiles\profiles.yml` please configure your dbt profile.
⚠️ Note: this location is **not** part of version control.  

Sample profile:

```
mini_data_warehouse:
  target: prod
  outputs:
    prod:
      account: <your snowflake account here>
      database: <your default database here>
      password: <your user password here>
      role: <your dbt default role here>
      schema: <your default schema here>
      threads: 3
      type: snowflake
      user: <your dbt user here>
      warehouse: <your default warehouse here>
```

Please make sure that the profile name `mini_data_warehouse` matches with the `profile` set under the `dbt_profile.yml` file.

Also, if you want this to try on a Snowflake account you either need to have an account already signed up or sign up for free trial. 

In case you don't want to deal with RBAC and security setup just test it with SYSADMIN I'd reccommend to set the default role to SYSADMIN 
and treat your environment temporary which can be teared down including transformed data.

Alternatively, I have another repository to provision custome roles and privileges [here](https://github.com/petero2018/TerraformSnowflake)
which can be utilised for RBAC and set least privileges.
---

## Step 3

- Navigate to the dbt project root foder `cd mini_data_warehouse\`.
- Run `make build`. This will build your docker image, install all dependancies, and configure a dbt environment.
- Run `make login`. This will open an interactive shell pointing to the dbt project.

The package manager is poetry within the container.
You have two option to run dbt from within the container:
1) you run `poetry run dbt debug`
2) you run `poetry shell` which provisions a virtual environment and then run `dbt debug`

`dbt debug` will run a test connection which will ensure you have the correct setup up and running.

If all when well you will be prompted on the screen and you can use dbt as you usually do from your computer.


The above setup ensures that the environment is portable and runs on all computer.

## Step 4

To execute the datapipeline, navigate to the dbt project folder and run `dbt build` which will run all seeds, models and tests.
To generate dbt docs, in the same dbt project folder, run `dbt docs generate` and then `dbt docs serve`. 
This will open up the dbt documentation which will provide a guide about pipelines, data cataloge and diagram.




Please note that I have collated my notes for completing the tasks within the [dbt Readme file](mini_data_warehouse/README.md)