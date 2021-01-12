# DataSketches Server
This is a very simple, container-friendly web server with a JSON API. The provided
server can be used to experiment with sketches or to add sketch functionality to a
project without natively integrating with the DataSketches library.

This is not intended to provide a high-performance database; there are already
existing integrations with PostgreSQL or Druid for such cases. The focus here 
is on ease-of-use rather than speed.

The repository is still in a very early state and lacks both unit tests and
robust documentation.

## Interaction

Configuration and interaction with the server are done via JSON. We will
demonstrate the main features of the server by creating a simple configuration
and using that for some example requests and responses.

Aside from configuration, all inputs may be sent as individual JSON objects or as
a JSON Array. Also, the server does not implement a transactional database. In the event
of an error partway through an array the server will return an error, but the
effects of any requests processed up to that point will be retained.

JSON input may be passed in via either POST or GET.


### Sketch Families

The server supports a variety of sketches suitable for different types of data analysis. For
full details of the sketches, see the main Apache DataSketches website documentation. The
key concepts used in the server are the sketch `family`, which determines the type of sketch, and
in some cases a value `type`, which is used to interpret the input values.

The known sketch families, the short name used in the server, and a description of the uses is
summarized in the following table:

| Short Name | Sketch Type      | Description |
| ---------- | ---------------- | ----------- |
| theta      | Theta Sketch     | Distinct counting with set operations |
| hll        | HyperLogLog      | Distinct counting, no set operations |
| cpc        | CPC              | Most compact distinct counting, no set operations |
| kll        | KLL              | Absolute error quantiles sketch of floating point values |
| frequency  | Frequent Items   | Heavy Hitters, items as strings |
| reservoir  | Reservoir Sample | Uniform, unweighted random sample, items as strings |
| varopt     | VarOpt Sample    | Weighted random sample for subset sum queries, items as strings |

The three distinct counting sketches also require the specification of a value type so that items
are presented to the sketch in a consistent way. The supported types are:

| Type Name | Description |
| --------- | ----------- |
| int       | 32-bit signed integer |
| long      | 64-bit signed integer |
| float     | 32-bit floating-point value |
| double    | 64-bit floating-point value |
| string    | Java String |


### Configuration

The configuration file is passed in at server creation time.  Aside from the port, the configuration file defines the
sketches held by the server. All sketches are instantiated at initialization, which keeps memory usage (mostly)
constrained during operation. Keep in mind that quantiles sketches do grow indefinitely, although very slowly as the
number of samples increases.

Examples are taken from [conf.json][example/conf.json].

There are two mechanisms for defining sketches. The first is an array of
fully-described entries. The key for such an array must have a prefix `sketches`:
```json
{
  "sketches_A": [
    { "name": "cpc_int",
      "k": 12,
      "type": "int",
      "family": "cpc"
    },
    { "name": "cpc_string",
      "k": 12,
      "type": "string",
      "family": "cpc"
    }
  ]
}
   ```
The above code creates two CPC sketches, `cpc_int` and `cpc_string`,
holding `int` and `string` data, respectively, both configured with a log2(k) size
parameter of 12. 

In order to create multiple sketches with different names but otherwise
configured identically, the key name must start with the prefix `set`:
```json
{
  "set1": {
    "k": 10,
    "family": "hll",
    "type": "double",
    "names": [
      "hll1",
      "hll2",
      "hll3"
    ]
  }
}
```
or
```json
{
  "set2": {
    "k": 12,
    "family": "theta",
    "type": "float",
    "names": [
      "theta0",
      "theta1",
      "theta2",
      "theta3",
      "theta4"
    ]
  }
}
```
The above examples create one set of 3 HLL sketches and one set of 5 theta sketches, respectively.

### Supported Operations

The available calls are:
* `/update`
* `/merge`
* `/query`
* `/serialize`
* `/status`

Each is described below, along with examples of input and output. As noted above, all calls accepting input may
be invoked with either a single JSON object or a JSON Array of such objects.

### Update

Adding data is perhaps the key operation of a sketch. Each update key is the target sketch's name, and the value is
either a single item or an array of items to be added to the sketch.

For sketches that accept weighted values, specifically Frequent Items and VarOpt Sampling, passing in an item without a
weight will be treated as having a weight of 1.0. To include a weight, the input must be a JSON object with both `item`
and `weight`. An array of values to update may include a mix of plain, unweighted values and weighted input objects.

An input to update may look like:
```json
{
    "cpc_int": [1, 2, 2, 3, 4, 5],
    "cpc_string": "single-item",
    "fi": ["abc",
	   { "item": "def", "weight": 3 },
	   { "item": "ghi", "weight": 2 },
	   "def"
	  ],
    "theta0": [1, 2, 3, 4, 5, 6],
    "theta1": [5, 6, 7, 8, 9, 10]
}
```
This example demonstrates the variety of inputs types, including repeated values, that are accepted by the server.

There is no result returned from a successful call to update aside from the standard status code 200.

```json
{
  "giveName": ["Alice", "Bob", "Cindy"], 
  "heightInches": [62, 70, 64],
  "postalCode": [12345, 11111, 94030]  
}
```

### Merge




### Query

### Serialize

### Status

A query to the `/status` page returns a list of the configured sketches. There is no input to this query.

Using `example/conf.json` to launch the server, a call to status returns:
```json
{
  "count": 11,
  "sketches": [
    {
      "name": "hll_3",
      "type": "double",
      "family": "hll"
    },
    {
      "name": "hll_4",
      "type": "double",
      "family": "hll"
    },
    {
      "name": "rs",
      "family": "reservoir"
    },
    {
      "name": "fi",
      "family": "frequency"
    },
    {
      "name": "hll_1",
      "type": "double",
      "family": "hll"
    },
    {
      "name": "hll_2",
      "type": "double",
      "family": "hll"
    },
    {
      "name": "cpc",
      "type": "string",
      "family": "cpc"
    },
    {
      "name": "theta_string",
      "type": "string",
      "family": "theta"
    },
    {
      "name": "theta_int",
      "type": "int",
      "family": "theta"
    },
    {
      "name": "vo",
      "family": "varopt"
    },
    {
      "name": "kll",
      "family": "kll"
    }
  ]
}
```

----

Disclaimer: Apache DataSketches is an effort undergoing incubation at The Apache
Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required
of all newly accepted projects until a further review indicates that the infrastructure,
communications, and decision making process have stabilized in a manner consistent
with other successful ASF projects. While incubation status is not necessarily a
reflection of the completeness or stability of the code, it does indicate that the
project has yet to be fully endorsed by the ASF.