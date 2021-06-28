# DataSketches Server
This is a very simple, container-friendly web server with a JSON API. The provided
server can be used to experiment with sketches or to add sketch functionality to a
project without natively integrating with the DataSketches library.

This is not intended to provide a high-performance database; there are already
existing integrations with PostgreSQL or Druid for such cases. The focus here 
is on ease-of-use rather than speed.

The repository is still in an early state and lacks both unit tests and
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
| string    | Java String (UTF-16) |


### Configuration

The configuration file is passed in at server creation time.  Aside from the port, the configuration file defines the
sketches held by the server. All sketches are instantiated at initialization, which keeps memory usage (mostly)
constrained during operation. Keep in mind that quantiles sketches do grow indefinitely, although very slowly as the
number of samples increases.

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

Finally, the port on which the server runs is specified with `port`:
```json
{
  "port": 8080
}
```

### Supported Operations

The available calls are:
* `/update`
* `/query`
* `/serialize`
* `/merge`
* `/status`

Each is described below, along with examples of input and output. As noted above, all calls accepting input may
be invoked with either a single JSON object or a JSON Array of such objects.

Examples are taken from the sketches configured in [conf.json][example/conf.json].


### Update

Adding data is perhaps the key operation of a sketch. Each update key is the target sketch's name, and the value is
either a single item or an array of items to be added to the sketch.

For sketches that accept weighted values, specifically Frequent Items and VarOpt Sampling, passing in an item without a
weight will be treated as having a weight of 1.0. To include a weight, the input must be a JSON object with both `item`
and `weight`. An array of values to update may include a mix of plain, unweighted values and weighted input objects.

An input to `/update`, taken from [update.json][example/update.json], may look like:
```json
{
    "cpcOfNumbers": [1, 2, 2, 3, 4, 5],
    "cpcOfStrings": "123e4567-e89b-12d3-a456-426655440000",
    "topItems": ["abc",
	   { "item": "def", "weight": 3 },
	   { "item": "ghi", "weight": 2 },
	   "def"
	  ],
  "duration": [502, 194, 793, 443, 1204, 892, 1075],
    "theta0": [1, 2, 3, 4, 5, 6],
    "theta1": [5, 6, 7, 8, 9, 10]
}
```
This example demonstrates the variety of inputs types, including repeated values, that are accepted by the server.

There is no result returned from a successful call to update aside from the standard status code 200.


### Query

Once the server has received input data, a `/query` request will produce estimates from one or more sketches.

A sample query may be found in [query.json][example/query.json]:
```json
[
  { "name": "topItems",
    "errorType": "noFalsePositives"
  },
  { "name": "duration",
    "resultType": "cdf",
    "values": [500, 800, 1000],
    "fractions": [0.2, 0.5, 0.8]
  },
  { "name": "cpcOfNumbers",
    "summary": false
  }
]
```
The example shows the use of an array to query multiple sketches with a single request. All sketches support
the `summary` property, which returns readable string with summary information about the sketch, although structured
primarily for debugging. The other properties supported in a query are specitic to the sketch type:

* theta, cpc, hll
  * No additional fields; returns all estimates
* kll
  * `resultType`: indicates `pmf` or `cdf` for rank results with a `values` query. A single
    query may include only one.
  * `values`: specifies split points in value space when querying ranks (as pmf or cdf)
  * `fractions`: specifies split points in rank space when querying values from the sketch   
* frequency
  * `errorType`: specifies `noFalsePositives` or `noFalseNegatives`
* varopt, reservoir
  * No additional fields; returns all items in sketch

Using the above query after the presented input returns
```json
[
  {
    "name": "topItems",
    "items": [
      {
        "item": "def",
        "estimate": 4,
        "upperBound": 4,
        "lowerBound": 4
      },
      {
        "item": "ghi",
        "estimate": 2,
        "upperBound": 2,
        "lowerBound": 2
      },
      {
        "item": "abc",
        "estimate": 1,
        "upperBound": 1,
        "lowerBound": 1
      }
    ]
  },
  {
    "name": "duration",
    "streamLength": 7,
    "estimationMode": false,
    "minValue": 194.0,
    "maxValue": 1204.0,
    "estimatedCDF": [
      {
        "value": 500.0,
        "rank": 0.2857142857142857
      },
      {
        "value": 800.0,
        "rank": 0.5714285714285714
      },
      {
        "value": 1000.0,
        "rank": 0.7142857142857143
      },
      {
        "value": 1204.0,
        "rank": 1.0
      }
    ],
    "estimatedQuantiles": [
      {
        "rank": 0.20000000298023224,
        "quantile": 443.0
      },
      {
        "rank": 0.5,
        "quantile": 793.0
      },
      {
        "rank": 0.800000011920929,
        "quantile": 1075.0
      }
    ]
  },
  {
    "name": "cpcOfNumbers",
    "estimate": 5.000946316025309,
    "estimationMode": true,
    "plus1StdDev": 6.0,
    "plus2StdDev": 6.0,
    "plus3StdDev": 6.0,
    "minus1StdDev": 5.0,
    "minus2StdDev": 5.0,
    "minus3StdDev": 5.0
  }
]
```

### Serialize

`/serialize` provides a way to extract a sketch from the server in a portable format. The serialized image is an
array of bytes, with the server returning a value using the standard URL-safe bas64 encoding. In addition to the
serialized string, the response includes the sketch family type and, for distinct counting sketches,, the type
of items the sketch accepts.

From [serialize.json][example/serialize.json], a request for several serialized results looks like:
```json
[
  { "name": "duration" },
  { "name": "cpcOfNumbers" }
]
```
After the update example above, the result (intentionally using results with short serialized strings) is:
```json
[
  {
    "name": "duration",
    "family": "KLL",
    "sketch": "BQEPAKAACAAHAAAAAAAAAKAAAQCZAAAAAABCQwCAlkQAYIZEAABfRACAlkQAgN1DAEBGRAAAQkMAAPtD"
  },
  {
    "name": "cpcOfNumbers",
    "family": "CPC",
    "type": "long",
    "sketch": "CAEQDAAOzJMFAAAAAgAAAAAAAABA_K9Ag4UxEvgAFEDW1VZChQZnQQ\u003d\u003d"
  }
]
```

Keep in mind that the serialized images may become quite large depending on the sketch configuration and
the number of items submitted to the sketch.


### Merge

All sketches included in the DataSketches library support merging. The server has a constraint on merging in that 
adding new sketches to an existing server is not supported; all sketches must be specified at initialization.
Merging supports two models: Merging into an existing sketch, and returning the serialized image of the resulting
sketch.

Again using the update example from earlier, we can call `/merge` using [merge.json][example/merge.json], where
the in-line image is 5 distinct items that do not overlap anything in [update.json][example/update.json]: 
```json
{ "target": "theta0",
  "source": [
      "theta0",
      "theta1",
      "theta2",
    {"family": "theta",
     "data":  "AgMDAAAazJMFAAAAAACAP_s4eYkTJI8BbakWvEpmYR4jpVs4Gv10Hz663KNvb7YgVx7EnK9Blzo\u003d" }
  ]
}
```

Upon a successful merge, there is no response beyond the standard 200 status code. The result can be seen by
querying the sketch. Doing so, we can see the expected result (with the sketch still in exact mode):
```json
{
  "name": "theta0",
  "estimate": 15.0,
  "estimationMode": false,
  "plus1StdDev": 15.0,
  "plus2StdDev": 15.0,
  "plus3StdDev": 15.0,
  "minus1StdDev": 15.0,
  "minus2StdDev": 15.0,
  "minus3StdDev": 15.0
}
```

### Status

A request to the `/status` page returns a list of the configured sketches. There is no input to this query.

Using [conf.json][example/conf.json] to launch the server, a call to `/status` returns:
```json
{
  "count": 15,
  "sketches": [
    {
      "name": "rs",
      "family": "reservoir"
    },
    {
      "name": "cpcOfNumbers",
      "type": "long",
      "family": "cpc"
    },
    {
      "name": "theta1",
      "type": "int",
      "family": "theta"
    },
    {
      "name": "theta0",
      "type": "int",
      "family": "theta"
    },
    {
      "name": "theta4",
      "type": "int",
      "family": "theta"
    },
    {
      "name": "theta3",
      "type": "int",
      "family": "theta"
    },
    {
      "name": "theta2",
      "type": "int",
      "family": "theta"
    },
    {
      "name": "duration",
      "family": "kll"
    },
    {
      "name": "cpcOfStrings",
      "type": "string",
      "family": "cpc"
    },
    {
      "name": "hll1",
      "type": "string",
      "family": "hll"
    },
    {
      "name": "vo",
      "family": "varopt"
    },
    {
      "name": "hll2",
      "type": "string",
      "family": "hll"
    },
    {
      "name": "hll3",
      "type": "string",
      "family": "hll"
    },
    {
      "name": "hll4",
      "type": "string",
      "family": "hll"
    },
    {
      "name": "topItems",
      "family": "frequency"
    }
  ]
}
```
