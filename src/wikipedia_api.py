import wikitextparser
import aiohttp

async def get_wikipedia_page(page_id, revision_id):
    """
    Fetches the title and content of a Wikipedia page given its page_id and revision_id using the Wikipedia API.

    Params:
    page_id (int): The ID of the Wikipedia page to be retrieved.
    revision_id (int): The ID of the specific revision of the page.

    Returns:
    A tuple containing the title and cleaned content of the Wikipedia page.
    If the page or revision is not found, returns an error message instead.
    """
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "revids": revision_id,
        "rvprop": "content"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            # Check if the request was successful
            response.raise_for_status()
            data = await response.json()
    
    # Check if the response contains the expected data
    try:
        page_title = data["query"]["pages"][str(page_id)]["title"]
        page_content = data["query"]["pages"][str(page_id)]["revisions"][0]["*"]
        parsed_content = wikitextparser.parse(page_content).plain_text().split("== See also ==")[0].lower()
        return r"{}".format(page_title), r"{}".format(parsed_content)
    except KeyError:
        return "Page or revision not found."
