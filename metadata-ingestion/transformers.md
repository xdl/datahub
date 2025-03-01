# Using transformers

## What’s a transformer?

Oftentimes we want to modify metadata before it reaches the ingestion sink – for instance, we might want to add custom tags, ownership, or patch some fields. A transformer allows us to do exactly these things.

Moreover, a transformer allows one to have fine-grained control over the metadata that’s ingested without having to modify the ingestion framework's code yourself. Instead, you can write your own module that can take MCEs however you like. To configure the recipe, all that's needed is a module name as well as any arguments.

## Provided transformers

Aside from the option of writing your own transformer (see below), we provide two simple transformers for the use cases of adding dataset tags and ownership information.

### Adding a set of tags

Let’s suppose we’d like to add a set of dataset tags. To do so, we can use the `simple_add_dataset_tags` module that’s included in the ingestion framework.

The config, which we’d append to our ingestion recipe YAML, would look like this:

```yaml
transformers:
  - type: "simple_add_dataset_tags"
    config:
      tag_urns:
        - "urn:li:tag:NeedsDocumentation"
        - "urn:li:tag:Legacy"
```

If you'd like to add more complex logic for assigning tags, you can use the more generic add_dataset_tags transformer, which calls a user-provided function to determine the tags for each dataset.

```yaml
transformers:
  - type: "add_dataset_tags"
    config:
      get_tags_to_add: "<your_module>.<your_function>"
```
### Change owners

If we wanted to clear existing owners sent by ingestion source we can use the `simple_remove_dataset_ownership` module which removes all owners sent by the ingestion source.

```yaml
transformers:
  - type: "simple_remove_dataset_ownership"
    config: {}
```

The main use case of `simple_remove_dataset_ownership` is to remove incorrect owners present in the source. You can use it along with the next `simple_add_dataset_ownership` to remove wrong owners and add the correct ones.

Let’s suppose we’d like to append a series of users who we know to own a dataset but aren't detected during normal ingestion. To do so, we can use the `simple_add_dataset_ownership` module that’s included in the ingestion framework.

The config, which we’d append to our ingestion recipe YAML, would look like this:

```yaml
transformers:
  - type: "simple_add_dataset_ownership"
    config:
      owner_urns:
        - "urn:li:corpuser:username1"
        - "urn:li:corpuser:username2"
        - "urn:li:corpGroup:groupname"
      ownership_type: "PRODUCER"
```

Note `ownership_type` is an optional field with `DATAOWNER` as default value.

### Setting ownership by dataset urn pattern

Let’s suppose we’d like to append a series of users who we know to own different dataset from a data source but aren't detected during normal ingestion. To do so, we can use the `pattern_add_dataset_ownership` module that’s included in the ingestion framework. it match pattern with `urn` of dataset and assign the respective owners

The config, which we’d append to our ingestion recipe YAML, would look like this:

```yaml
transformers:
  - type: "pattern_add_dataset_ownership"
    config:
      owner_pattern:
        rules:
          ".*example1.*": ["urn:li:corpuser:username1"]
          ".*example2.*": ["urn:li:corpuser:username2"]
      ownership_type: "DEVELOPER"
```

Note `ownership_type` is an optional field with `DATAOWNER` as default value.

If you'd like to add more complex logic for assigning ownership, you can use the more generic `add_dataset_ownership` transformer, which calls a user-provided function to determine the ownership of each dataset.

```yaml
transformers:
  - type: "add_dataset_ownership"
    config:
      get_owners_to_add: "<your_module>.<your_function>"
```

Note that whatever owners you send via this will overwrite the owners present in the UI.

### Mark dataset status

If you would like to stop a dataset from appearing in the UI then you need to mark the status of the dataset as removed. You can use this transformer after filtering for the specific datasets that you want to mark as removed.

```yaml
transformers:
  - type: "mark_dataset_status"
    config:
      removed: true
```

### Add dataset browse paths

If you would like to add to browse paths of dataset can use this transformer. There are 3 optional variables that you can use to get information from the dataset `urn`:
- ENV: env passed (default: prod)
- PLATFORM: `mysql`, `postgres` or different platform supported by datahub
- DATASET_PARTS: slash separated parts of dataset name. e.g. `database_name/schema_name/[table_name]` for postgres

e.g. this can be used to create browse paths like `/prod/postgres/superset/public/logs` for table `superset.public.logs` in a `postgres` database
```yaml
transformers:
  - type: "set_dataset_browse_path"
    config:
      path_templates:
        - /ENV/PLATFORM/DATASET_PARTS/ 
```

If you don't want the environment but wanted to add something static in the browse path like the database instance name you can use this.
```yaml
transformers:
  - type: "set_dataset_browse_path"
    config:
      path_templates:
        - /PLATFORM/marketing_db/DATASET_PARTS/ 
```
It will create browse path like `/mysql/marketing_db/sales/orders` for a table `sales.orders` in `mysql` database instance.

You can use this to add multiple browse paths. Different people might know same data assets with different name
```yaml
transformers:
  - type: "set_dataset_browse_path"
    config:
      path_templates:
        - /PLATFORM/marketing_db/DATASET_PARTS/
        - /data_warehouse/DATASET_PARTS/
```
This will add 2 browse paths like `/mysql/marketing_db/sales/orders` and `/data_warehouse/sales/orders` for a table `sales.orders` in `mysql` database instance.

Default behaviour of the transform is to add new browse paths, you can optionally set `replace_existing: True` so 
the transform becomes a _set_ operation instead of an _append_.
```yaml
transformers:
  - type: "set_dataset_browse_path"
    config:
      replace_existing: True
      path_templates:
        - /ENV/PLATFORM/DATASET_PARTS/
```
In this case, the resulting dataset will have only 1 browse path, the one from the transform.

Note that whatever browse paths you send via this will overwrite the browse paths present in the UI.
## Writing a custom transformer from scratch

In the above couple of examples, we use classes that have already been implemented in the ingestion framework. However, it’s common for more advanced cases to pop up where custom code is required, for instance if you'd like to utilize conditional logic or rewrite properties. In such cases, we can add our own modules and define the arguments it takes as a custom transformer.

As an example, suppose we want to append a set of ownership fields to our metadata that are dependent upon an external source – for instance, an API endpoint or file – rather than a preset list like above. In this case, we can set a JSON file as an argument to our custom config, and our transformer will read this file and append the included ownership classes to all our MCEs (if you'd like, you could also include filtering logic for specific MCEs).

Our JSON file might look like the following:

```json
[
  "urn:li:corpuser:athos",
  "urn:li:corpuser:porthos",
  "urn:li:corpuser:aramis",
  "urn:li:corpGroup:the_three_musketeers"
]
```

### Defining a config

To get started, we’ll initiate an `AddCustomOwnershipConfig` class that inherits from [`datahub.configuration.common.ConfigModel`](./src/datahub/configuration/common.py). The sole parameter will be an `owners_json` which expects a path to a JSON file containing a list of owner URNs. This will go in a file called `custom_transform_example.py`.

```python
from datahub.configuration.common import ConfigModel

class AddCustomOwnershipConfig(ConfigModel):
    owners_json: str
```

### Defining the transformer

Next, we’ll define the transformer itself, which must inherit from [`datahub.ingestion.api.transform.Transformer`](./src/datahub/ingestion/api/transform.py). First, let's get all our imports in:

```python
# append these to the start of custom_transform_example.py

import json
from typing import Iterable

# for constructing URNs
import datahub.emitter.mce_builder as builder
# for typing the config model
from datahub.configuration.common import ConfigModel
# for typing context and records
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
# base transformer class
from datahub.ingestion.api.transform import Transformer
# MCE-related classes
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
```

Next, let's define the base scaffolding for the class:

```python
# append this to the end of custom_transform_example.py

class AddCustomOwnership(Transformer):
    """Transformer that adds owners to datasets according to a callback function."""

    # context param to generate run metadata such as a run ID
    ctx: PipelineContext
    # as defined in the previous block
    config: AddCustomOwnershipConfig

    def __init__(self, config: AddCustomOwnershipConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

        self.owners = [
            OwnerClass(owner=owner, type=OwnershipTypeClass.DATAOWNER)
            for owner in json.loads(config.owner_file)
        ]
```

A transformer must have two functions: a `create()` function for initialization and a `transform()` function for executing the transformation.

Let's begin by adding a `create()` method for parsing our configuration dictionary:

```python
# add this as a function of AddCustomOwnership

@classmethod
def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddCustomOwnership":
    config = AddCustomOwnershipConfig.parse_obj(config_dict)
    return cls(config, ctx)
```

Now we need to add a `transform()` method that does the work of adding our custom ownership classes. This method will take an MCE as input and output the transformed MCE. Let's offload the processing of each MCE to another `transform_one()` class.

```python
# add this as a function of AddCustomOwnership

def transform(
    self, record_envelopes: Iterable[RecordEnvelope]
) -> Iterable[RecordEnvelope]:

    # loop over envelopes
    for envelope in record_envelopes:

        # if envelope is an MCE, add the ownership classes
        if isinstance(envelope.record, MetadataChangeEventClass):
            envelope.record = self.transform_one(envelope.record)
        yield envelope
```

With the main `transform()` method set up, the `transform_one()` method will take a single MCE and add the owners that we loaded from the JSON.

```python
# add this as a function of AddCustomOwnership

def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
    if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
        return mce

    owners_to_add = self.owners

    if owners_to_add:
        ownership = builder.get_or_add_aspect(
            mce,
            OwnershipClass(
                owners=[],
            ),
        )
        ownership.owners.extend(owners_to_add)

    return mce
```

### Installing the package

Now that we've defined the transformer, we need to make it visible to DataHub. The easiest way to do this is to just place it in the same directory as your recipe, in which case the module name is the same as the file – in this case, `custom_transform_example`.

<details>
  <summary>Advanced: installing as a package</summary>
Alternatively, create a `setup.py` in the same directory as our transform script to make it visible globally. After installing this package (e.g. with `python setup.py` or `pip install -e .`), our module will be installed and importable as `custom_transform_example`.

```python
from setuptools import find_packages, setup

setup(
    name="custom_transform_example",
    version="1.0",
    packages=find_packages(),
    # if you don't already have DataHub installed, add it under install_requires
	# install_requires=["acryl-datahub"]
)
```

</details>

### Running the transform

```yaml
transformers:
  - type: "custom_transform_example.AddCustomOwnership"
    config:
      owners_json: "<path_to_owners_json>" # the JSON file mentioned at the start
```

After running `datahub ingest -c <path_to_recipe>`, our MCEs will now have the following owners appended:

```json
"owners": [
    {
        "owner": "urn:li:corpuser:athos",
        "type": "DATAOWNER",
        "source": null
    },
    {
        "owner": "urn:li:corpuser:porthos",
        "type": "DATAOWNER",
        "source": null
    },
    {
        "owner": "urn:li:corpuser:aramis",
        "type": "DATAOWNER",
        "source": null
    },
    {
        "owner": "urn:li:corpGroup:the_three_musketeers",
        "type": "DATAOWNER",
        "source": null
    },
	// ...and any additional owners
],
```

All the files for this tutorial may be found [here](./examples/transforms/).
