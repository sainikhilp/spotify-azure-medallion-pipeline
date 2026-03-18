# Databricks notebook source
parameters = [
    {
        "table":"spotify_cata.silver.factstream",
        "cols": "factstream.stream_id,factstream.listen_duration",
        "alias": "factstream"
    },
    {
        "table":"spotify_cata.silver.dimuser",
        "cols": "dimuser.user_id,dimuser.user_name",
        "alias": "dimuser",
        "condition": "factstream.user_id = dimuser.user_id"
    },
    {
        "table":"spotify_cata.silver.dimtrack",
        "cols": "dimtrack.track_id,dimtrack.track_name",
        "alias": "dimtrack",
        "condition": "factstream.track_id = dimtrack.track_id"
    }
]

# COMMAND ----------

pip install jinja2

# COMMAND ----------

from jinja2 import Template

# COMMAND ----------

print("Hello")

# COMMAND ----------

query_text = '''
    SELECT
    {% for param in parameters %}
        {{ param.cols }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM
    {% for param in parameters %}
        {% if loop.first %}
            {{ param.table }} AS {{ param.alias }}
        {% endif %}
    {% endfor %}
{% for param in parameters %}
    {% if not loop.first %}
        LEFT JOIN {{ param.table }} AS {{ param.alias }}
            ON {{ param.condition }}
    {% endif %}
{% endfor %}

'''


# COMMAND ----------

jinja_sql_str = Template(query_text)
query = jinja_sql_str.render(paramters=parameters)
print(query)

# COMMAND ----------



# COMMAND ----------

display(spark.sql(query))

# COMMAND ----------

