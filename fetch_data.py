import requests
import wikitextparser

def get_wikipedia_page(page_id, revision_id):
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "revids": revision_id,
        "rvprop": "content"
    }

    response = requests.get(url, params=params)
    data = response.json()
    
    # Check if the response contains the expected data
    try:
        page_content = data["query"]["pages"][str(page_id)]["revisions"][0]["*"]
        return page_content
    except KeyError:
        return "Page or revision not found."

# Example usage:
page_id = 1397  # Example page ID
revision_id = 230137091  # Example revision ID
content = get_wikipedia_page(page_id, revision_id)
print(wikitextparser.parse(content).plain_text().split("== See also ==")[0].lower())
