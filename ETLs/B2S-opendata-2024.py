# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

#   Input path

Bronze_opendata_path = generate_path('ENRICHDATA','bronzelayer')

#   Output paths

Silver_path = generate_path('raw-deltaOPENDATA','silverlayer')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloader, extract date of filename

# COMMAND ----------

# Read JSON file from the bronze layer
bronze_df = spark.read.json(
    f"{Bronze_opendata_path}/Ads-by-District.json",
    multiLine=True
)

# Display the DataFrame
display(bronze_df)

# Save the DataFrame to the silver layer with overwrite mode
bronze_df.write.format("delta").mode("overwrite").save(
    f"{Silver_path}/Ads-by-District"
)

# COMMAND ----------

# Read JSON file from the bronze layer
bronze_properties_df = spark.read.json(f"{Bronze_opendata_path}/properties_by_district.json", multiLine=True)

# Display the DataFrame
display(bronze_properties_df)

# Save the DataFrame to the silver layer
bronze_properties_df.write.format("delta").mode("overwrite").save(f"{Silver_path}/properties_by_district")

# COMMAND ----------

# Read CSV file from the specified path in the bronze layer with ';' as the separator
bronze_unemployment_df = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/desempleoINE/nac-ccaa_tasa_de_paro_de_las_personas_entre_16_y_74_anos_por_grupo_de_edad_y_comunidad_autonoma(67566)/csv_bdsc.csv",
    header=True
)

# Rename columns to remove invalid characters
for col_name in bronze_unemployment_df.columns:
    new_col_name = col_name.replace(" ", "_").replace(";", "_").replace("{", "_").replace("}", "_").replace("(", "_").replace(")", "_").replace("\n", "_").replace("\t", "_").replace("=", "_")
    bronze_unemployment_df = bronze_unemployment_df.withColumnRenamed(col_name, new_col_name)

# Display the DataFrame
display(bronze_unemployment_df)

# Save the DataFrame to the silver layer
bronze_unemployment_df.write.mode("overwrite").format("delta").save(
    f"{Silver_path}/unemployment_by_age_and_region"
)

# COMMAND ----------

# Read CSV file from the specified path in the bronze layer with ';' as the separator
bronze_employment_df = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/empleoINE/nac-ccaa_tasa_de_empleo_de_las_personas_entre_20_y_64_anos_por_grupo_de_edad_y_comunidad_autonoma(67557)/csv_bdsc.csv",
    header=True
)

# Rename columns to remove invalid characters
for col_name in bronze_employment_df.columns:
    new_col_name = col_name.replace(" ", "_").replace(";", "_").replace("{", "_").replace("}", "_").replace("(", "_").replace(")", "_").replace("\n", "_").replace("\t", "_").replace("=", "_")
    bronze_employment_df = bronze_employment_df.withColumnRenamed(col_name, new_col_name)

# Display the DataFrame
display(bronze_employment_df)

# Save the DataFrame to the silver layer
bronze_employment_df.write.format("delta").mode("overwrite").save(
    f"{Silver_path}/employment_by_age_and_region"
)

# COMMAND ----------

# Read CSV files from the specified path in the bronze layer with ';' as the separator
bronze_indicadores_hogares_df1 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/indicadoreshogaresINE/cuenta_de_asignacion_de_la_renta_primaria_por_comunidades_y_ciudades_autonomas,_recursos/empleos_y_periodo._(tpx_67301)/csv_bdsc.csv",
    header=True
)

bronze_indicadores_hogares_df2 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/indicadoreshogaresINE/cuenta_de_distribucion_secundaria_de_la_renta_por_comunidades_y_ciudades_autonomas,_recursos/empleos_y_periodo._(tpx_67302)/csv_bdsc.csv",
    header=True
)

bronze_indicadores_hogares_df3 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/indicadoreshogaresINE/cuenta_de_redistribucion_de_la_renta_en_especie_por_comunidades_y_ciudades_autonomas,_recursos/empleos_y_periodo._(tpx_67303)/csv_bdsc.csv",
    header=True
)

bronze_indicadores_hogares_df4 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/indicadoreshogaresINE/rev2019_nac_saldos_contables(67280)/csv_bdsc.csv",
    header=True
)

# Rename columns to remove invalid characters for each DataFrame
for df in [bronze_indicadores_hogares_df1, bronze_indicadores_hogares_df2, bronze_indicadores_hogares_df3, bronze_indicadores_hogares_df4]:
    for col_name in df.columns:
        new_col_name = col_name.replace(" ", "_").replace(";", "_").replace("{", "_").replace("}", "_").replace("(", "_").replace(")", "_").replace("\n", "_").replace("\t", "_").replace("=", "_")
        df = df.withColumnRenamed(col_name, new_col_name)

# Display the DataFrames
display(bronze_indicadores_hogares_df1)
display(bronze_indicadores_hogares_df2)
display(bronze_indicadores_hogares_df3)
display(bronze_indicadores_hogares_df4)

# COMMAND ----------



-

-

-

-

-

-

-
rev2019_nac_pcorr_agregados_por_ramas_de_actividad(67195)
-
rev2019_nac_pcorr_formacion_bruta_de_capital_fijo_por_activos_y_ramas_de_actividad(67199)
-
rev2019_nac_pcorr_valor_anadido_bruto(67197)

# COMMAND ----------

# Read CSV files from the specified path in the bronze layer with ';' as the separator
bronze_pib_df1 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/empleo_total_y_asalariado__resultados_por_provincias,_ramas_de_actividad,_magnitud_y_periodo._(tpx_67300)/csv_bdsc.csv",
    header=True
)

bronze_pib_df2 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/gasto_en_consumo_final_de_los_hogares_(precios_corrientes)_por_comunidad_autonoma,_divisiones_coicop_y_ano._(tpx_67298)/csv_bdsc.csv",
    header=True
)

bronze_pib_df3 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/nac_ceec-pcorr_pib_pm_oferta_(precios_corrientes)(67196)/csv_bdsc.csv",
    header=True
)

bronze_pib_df4 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/nac_ceec-pcorr_pib_pm_rentas_(precios_corrientes)(67200)/csv_bdsc.csv",
    header=True
)

bronze_pib_df5 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/p.i.b._a_precios_de_mercado_y_valor_anadido_bruto_a_precios_basicos_por_ramas_de_actividad__precios_corrientes_por_comunidades_y_ciudades_autonomas,_magnitud_y_periodo._(tpx_67295)/csv_bdsc.csv",
    header=True
)

bronze_pib_df6 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/p.i.b._a_precios_de_mercado_y_valor_anadido_bruto_a_precios_basicos_por_ramas_de_actividad__precios_corrientes_por_provincias_y_periodo._(tpx_67296)/csv_bdsc.csv",
    header=True
)

bronze_pib_df7 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/p.i.b._a_precios_de_mercado_y_valor_anadido_bruto_a_precios_basicos_por_ramas_de_actividad_por_comunidades_y_ciudades_autonomas,_magnitud_y_periodo._(tpx_67297)/csv_bdsc.csv",
    header=True
)

bronze_pib_df8 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/rev2019_nac_ceec_empleo_por_ramas_de_actividad(67201)/csv_bdsc.csv",
    header=True
)

bronze_pib_df9 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/rev2019_nac_ceec_pib_pm_demanda_(indices_de_volumen_encadenado)(67198)/csv_bdsc.csv",
    header=True
)

bronze_pib_df10 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/rev2019_nac_empleo(67202)/csv_bdsc.csv",
    header=True
)

bronze_pib_df11 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/rev2019_nac_pcorr_agregados_por_ramas_de_actividad(67195)/csv_bdsc.csv",
    header=True
)

bronze_pib_df12 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/rev2019_nac_pcorr_formacion_bruta_de_capital_fijo_por_activos_y_ramas_de_actividad(67199)/csv_bdsc.csv",
    header=True
)

bronze_pib_df13 = spark.read.option("delimiter", ";").csv(
    f"{Bronze_opendata_path}/PIBine/rev2019_nac_pcorr_valor_anadido_bruto(67197)/csv_bdsc.csv",
    header=True
)


# Rename columns to remove invalid characters for each DataFrame
for df in [bronze_pib_df1, bronze_pib_df2, bronze_pib_df3, bronze_pib_df4, bronze_pib_df5, bronze_pib_df6,bronze_pib_df7, bronze_pib_df8, bronze_pib_df9, bronze_pib_df10, bronze_pib_df11, bronze_pib_df12, bronze_pib_df13]:
    for col_name in df.columns:
        new_col_name = col_name.replace(" ", "_").replace(";", "_").replace("{", "_").replace("}", "_").replace("(", "_").replace(")", "_").replace("\n", "_").replace("\t", "_").replace("=", "_")
        df = df.withColumnRenamed(col_name, new_col_name)

# Display the DataFrames
display(bronze_pib_df1)
display(bronze_pib_df2)
display(bronze_pib_df3)
display(bronze_pib_df4)
display(bronze_pib_df5)
display(bronze_pib_df6)
display(bronze_pib_df7)
display(bronze_pib_df8)
display(bronze_pib_df9)
display(bronze_pib_df10)
display(bronze_pib_df11)
display(bronze_pib_df12)
display(bronze_pib_df13)


# COMMAND ----------



# COMMAND ----------

# Define the list of file paths
file_paths = [
    "PIBine empleo_total_y_asalariado__resultados_por_provincias,_ramas_de_actividad,_magnitud_y_periodo._(tpx_67300)",
    "gasto_en_consumo_final_de_los_hogares_(precios_corrientes)_por_comunidad_autonoma,_divisiones_coicop_y_ano._(tpx_67298)",
    "nac_ceec-pcorr_pib_pm_oferta_(precios_corrientes)(67196)",
    "nac_ceec-pcorr_pib_pm_rentas_(precios_corrientes)(67200)",
    "p.i.b._a_precios_de_mercado_y_valor_anadido_bruto_a_precios_basicos_por_ramas_de_actividad__precios_corrientes_por_comunidades_y_ciudades_autonomas,_magnitud_y_periodo._(tpx_67295)",
    "p.i.b._a_precios_de_mercado_y_valor_anadido_bruto_a_precios_basicos_por_ramas_de_actividad__precios_corrientes_por_provincias_y_periodo._(tpx_67296)",
    "p.i.b._a_precios_de_mercado_y_valor_anadido_bruto_a_precios_basicos_por_ramas_de_actividad_por_comunidades_y_ciudades_autonomas,_magnitud_y_periodo._(tpx_67297)",
    "rev2019_nac_ceec_empleo_por_ramas_de_actividad(67201)",
    "rev2019_nac_ceec_pib_pm_demanda_(indices_de_volumen_encadenado)(67198)",
    "rev2019_nac_empleo(67202)",
    "rev2019_nac_pcorr_agregados_por_ramas_de_actividad(67195)",
    "rev2019_nac_pcorr_formacion_bruta_de_capital_fijo_por_activos_y_ramas_de_actividad(67199)",
    "rev2019_nac_pcorr_valor_anadido_bruto(67197)"
]

# Read CSV files from the specified paths in the bronze layer with ';' as the separator
bronze_dfs = []
for file_path in file_paths:
    directory_path = f"{Bronze_opendata_path}/{file_path}"
    full_path = f"{directory_path}/csv_bdsc.csv"
    if dbutils.fs.ls(directory_path):
        df = spark.read.option("delimiter", ";").csv(
            full_path,
            header=True
        )
        # Rename columns to remove invalid characters
        for col_name in df.columns:
            new_col_name = col_name.replace(" ", "_").replace(";", "_").replace("{", "_").replace("}", "_").replace("(", "_").replace(")", "_").replace("\n", "_").replace("\t", "_").replace("=", "_")
            df = df.withColumnRenamed(col_name, new_col_name)
        bronze_dfs.append(df)

# Display the DataFrames
for df in bronze_dfs:
    display(df)

# COMMAND ----------

# Define the list of new file paths
new_file_paths = [
    "nac_poblacion_por_sexo,_edad_(ano_a_ano)_y_pais_de_nacimiento_(agrupacion_de_paises_por_nivel_de_desarrollo)(69576)",
    "nac_poblacion_por_sexo,_edad_(ano_a_ano)_y_pais_de_nacionalidad_(agrupacion_de_paises_por_nivel_de_desarrollo)(69575)",
    "nac-ccaa-prov_nivel_de_estudios_completados__poblacion_de_15_y_mas_anos_por_sexo,_edad_y_nivel_de_estudios_(detalle)_(67573)",
    "prov_edad_mediana_de_la_poblacion_por_provincia,_segun_sexo(67304)",
    "prov_tasa_de_dependencia_de_la_poblacion_mayor_de_64_anos,_por_provincia(67305)"
]

# Read CSV files from the specified paths in the bronze layer with ';' as the separator
new_bronze_dfs = []
for file_path in new_file_paths:
    df = spark.read.option("delimiter", ";").csv(
        f"{Bronze_opendata_path}/{file_path}/csv_bdsc.csv",
        header=True
    )
    # Rename columns to remove invalid characters
    for col_name in df.columns:
        new_col_name = col_name.replace(" ", "_").replace(";", "_").replace("{", "_").replace("}", "_").replace("(", "_").replace(")", "_").replace("\n", "_").replace("\t", "_").replace("=", "_")
        df = df.withColumnRenamed(col_name, new_col_name)
    new_bronze_dfs.append(df)

# Display the DataFrames
for df in new_bronze_dfs:
    display(df)

# COMMAND ----------

# Define the list of new file paths
tourism_file_paths = [
    "ccaa_viajeros_y_pernoctaciones_por_comunidades_autonomas(67191)",
    "ccaa_viajeros_y_pernoctaciones_por_comunidades_autonomas(67192)",
    "nac_personas_viajeras_por_sexo,_segun_destino_y_duracion_del_viaje.(67186)",
    "nac-ccaa_viajeros_y_pernoctaciones_por_comunidades_autonomas(67194)",
    "nac-ccaa_viajeros_y_pernoctaciones_por_comunidades_y_ciudades_autonomas(67193)",
    "nac-ccaa-prov_viajeros_y_pernoctaciones_por_comunidades_autonomas_y_provincias(67190)"
]

# Read CSV files from the specified paths in the bronze layer with ';' as the separator
tourism_bronze_dfs = []
for file_path in tourism_file_paths:
    df = spark.read.option("delimiter", ";").csv(
        f"{Bronze_opendata_path}/{file_path}/csv_bdsc.csv",
        header=True
    )
    # Rename columns to remove invalid characters
    for col_name in df.columns:
        new_col_name = col_name.replace(" ", "_").replace(";", "_").replace("{", "_").replace("}", "_").replace("(", "_").replace(")", "_").replace("\n", "_").replace("\t", "_").replace("=", "_")
        df = df.withColumnRenamed(col_name, new_col_name)
    tourism_bronze_dfs.append(df)

# Display the DataFrames
for df in tourism_bronze_dfs:
    display(df)
