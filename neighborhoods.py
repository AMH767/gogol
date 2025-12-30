# Dictionary of major cities and their neighborhoods/districts for granular searching
CITY_SUBREGIONS = {
    "New York": [
        "Manhattan", "Brooklyn", "Queens", "The Bronx", "Staten Island",
        "Upper East Side", "Upper West Side", "Midtown", "Lower Manhattan", "Harlem",
        "Williamsburg", "Bushwick", "DUMBO", "Astoria", "Flushing", "Long Island City"
    ],
    "Los Angeles": [
        "Downtown LA", "Hollywood", "Santa Monica", "Venice", "Beverly Hills",
        "Silver Lake", "Echo Park", "Koreatown", "Westwood", "Sherman Oaks"
    ],
    "Chicago": [
        "The Loop", "Lincoln Park", "Wicker Park", "Logan Square", "River North",
        "Hyde Park", "West Loop", "Lakeview"
    ],
    "London": [
        "Westminster", "Camden", "Islington", "Hackney", "Southwark", "Lambeth",
        "Greenwich", "Chelsea", "Kensington", "Soho", "Shoreditch"
    ],
    "Moscow": [
        "Центральный округ", "Северный округ", "Северо-Восточный округ", "Восточный округ",
        "Юго-Восточный округ", "Южный округ", "Юго-Западный округ", "Западный округ",
        "Северо-Западный округ", "Зеленоград", "Хамовники", "Пресненский", "Арбат"
    ]
}

def get_subregions(query):
    """
    Attempts to identify a city in the query and returns its subregions.
    Example: "Pizza New York" -> returns neighborhoods of New York.
    """
    if not query:
        return []
    query_lower = query.lower()
    for city, subregions in CITY_SUBREGIONS.items():
        if city.lower() in query_lower:
            return subregions
    return []
