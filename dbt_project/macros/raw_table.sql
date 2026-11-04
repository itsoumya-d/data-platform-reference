{% macro raw_table(name) %}
{% if target.type == 'duckdb' %}
read_parquet('{{ var("raw_dir") }}/{{ name }}.parquet')
{% else %}
RAW.{{ name | upper }}
{% endif %}
{% endmacro %}
