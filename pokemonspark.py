from pyspark.sql import SparkSession

def parse_and_even(line):
    p = line.split(",")
    p_id = int(p[0])
    if p_id % 2 == 0:
        return (p[1], int(p[2])) # Return (Name, Attack)
    return None

def double_attack(p):
    return (p[0], p[1] * 2)

def square_attack(p):
    return (p[0], p[1] ** 2)

def shift_attack(p):
    return (p[0], p[1] + 10)

def filter_strong(p):
    return p[1] > 50

def main():
    spark = SparkSession.builder.appName("PokemonNumericLogic").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Simulation of IDs 1 to 20 (Matches numbers = parallelize(range(1, 21)))
    raw_pokemon = [
        "2,Ivysaur,62", "4,Charmander,52", "6,Charizard,84", "8,Wartortle,63",
        "10,Caterpie,30", "12,Butterfree,45", "14,Kakuna,25", "16,Pidgey,45",
        "18,Pidgeot,80", "20,Raticate,81"
    ]
    pokemon_rdd = sc.parallelize(raw_pokemon)

    # 1. Keep only even IDs (Filter)
    # 2. Double the Attack (Map)
    # 3. Square the Attack (Map)
    # 4. Add 10 to Attack (Map)
    # 5. Keep only values > 50 (Filter)
    
    final_results = pokemon_rdd.map(parse_and_even) \
                               .filter(lambda x: x is not None) \
                               .map(double_attack) \
                               .map(square_attack) \
                               .map(shift_attack) \
                               .filter(filter_strong)

    # 6. Sort descending (Matches sorted_desc)
    sorted_desc = final_results.sortBy(lambda x: x[1], ascending=False)

    print("\nFinal Pokémon results (sorted by transformed Attack):")
    for p in sorted_desc.collect():
        print(f"Name: {p[0]} | Transformed Attack: {p[1]}")

    print("\nCount of remaining Pokémon:", final_results.count())
    spark.stop()

if __name__ == "__main__":
    main()