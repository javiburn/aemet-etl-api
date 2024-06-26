/**
 * Función para convertir las columnas a los tipos correctos
 */
function convert_columns_to_correct_type(dataframe) {
  console.log("convert_columns_to_correct_type: Function called");
  // Verifica si la columna 'hrmedia' está presente antes de intentar convertirla
  if (dataframe.schema.fieldNames().includes('hrmedia')) {
    console.log("hrmedia column found, converting types");
    dataframe = dataframe
        .withColumn("fecha", dataframe.col("fecha").cast("timestamp"))
        .withColumn("altitud", dataframe.col("altitud").cast("double"))
        .withColumn("tmed", dataframe.col("tmed").cast("double"))
        .withColumn("prec", dataframe.col("prec").cast("double"))
        .withColumn("tmin", dataframe.col("tmin").cast("double"))
        .withColumn("tmax", dataframe.col("tmax").cast("double"))
        .withColumn("dir", dataframe.col("dir").cast("double"))
        .withColumn("velmedia", dataframe.col("velmedia").cast("double"))
        .withColumn("racha", dataframe.col("racha").cast("double"))
        .withColumn("sol", dataframe.col("sol").cast("double"))
        .withColumn("presmax", dataframe.col("presmax").cast("double"))
        .withColumn("presmin", dataframe.col("presmin").cast("double"))
        .withColumn("hrmedia", dataframe.col("hrmedia").cast("double"))
        .withColumn("hrmax", dataframe.col("hrmax").cast("double"))
        .withColumn("hrmin", dataframe.col("hrmin").cast("double"));
  } else {
    console.log("hrmedia column not found");
  }
  return dataframe;
}

module.exports = {
  convert_columns_to_correct_type: convert_columns_to_correct_type
};
