from ingestion.sources.openaq import OpenAQClient

client = OpenAQClient()

try:
    print("\nConnecting to OpenAQ API...")

    locations = client.get_all_egypt_locations()

    print("\n========== OpenAQ TEST RESULTS ==========")

    print(f"Total Locations: {len(locations)}")

    print("\nFirst 5 Locations:")

    for location in locations[:5]:

        coordinates = location.get("coordinates") or {}

        print({
            "id": location.get("id"),
            "name": location.get("name"),
            "locality": location.get("locality"),
            "latitude": coordinates.get("latitude"),
            "longitude": coordinates.get("longitude")
        })

    print("\nOpenAQ Connection Test: SUCCESS")

except Exception as error:

    print("\nOpenAQ Connection Test: FAILED")
    print("Error:", error)

finally:
    client.close()
