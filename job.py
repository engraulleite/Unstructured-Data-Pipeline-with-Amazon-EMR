from pyspark.sql import SparkSession

# Função
def word_count(spark):
    
    # Carregar dados
    data = spark.sparkContext.textFile("s3://raul-emr-jobs/input/dataset.txt")
    
    # Contagem de palavras
    words = data.flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a + b)
    
    # Salvar a saída no S3
    wordCounts.saveAsTextFile("s3://raul-emr-jobs/output")

# Main
if __name__ == "__main__":
    
    # Inicializar SparkSession
    spark = SparkSession.builder.appName("raulJob").getOrCreate()
    
    # Executar a função word_count
    word_count(spark)
    
    # Parar a SparkSession
    spark.stop()
