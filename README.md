# DataSketches Server
This is a very simple, container-friendly web server with a JSON API. The provided
server can be used to experiment with sketches or to add sketch functionality to a
project without natively integrating with the DataSketches library.

This is not intended to provide a high-performance database; there are already
existing integrations with PostgreSQL or Druid for such cases. The focus here 
is on ease-of-use rather than speed.

The repository is still in a very early state and lacks both unit tests and
robust documentation.

----

Disclaimer: Apache DataSketches is an effort undergoing incubation at The Apache
Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required
of all newly accepted projects until a further review indicates that the infrastructure,
communications, and decision making process have stabilized in a manner consistent
with other successful ASF projects. While incubation status is not necessarily a
reflection of the completeness or stability of the code, it does indicate that the
project has yet to be fully endorsed by the ASF.