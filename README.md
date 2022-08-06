# think-tank-indexing
Think tank indexing is project focused to provide way to have fast and flexible searching abbilities for web content,
the data surce is https://www.cfr.org/

<p align="center">
  <img src="documentation/diagrams/Project%20Diagram.png" width="800" title="hover text">
</p>

## About CFR
The Council on Foreign Relations (CFR) is an American think tank specializing in U.S. foreign policy and international relations. Founded in 1921, it is a nonprofit organization that is independent and nonpartisan. CFR is based in New York City, with an additional office in Massachusetts. Its membership, which numbers 5,103, has included senior politicians, numerous secretaries of state, CIA directors, bankers, lawyers, professors, corporate directors and CEOs, and senior media figures.

## The end goal

The end goal would be to provide API end point which would return data basing on filter provided by end user,
for example:

SELECT Title
FROM Content
WHERE
    'Angela Merkel' IN Persons
    AND ('Petrol' OR 'Chemistry' OR 'Automotive') IN Topic
    AND PostDate <= '2021.01.01'

# 🤖 Tech stack
* Docker
* Elasticseach
* Kibana
* Dagster
* Minio
* Python
* pandas
* nltk
* spaCy