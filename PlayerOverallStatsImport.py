


def statPageCrawl(url, visited=None, word_dict=None):
    """a recursive web crawler that calls analyze()
       on every visited web page; pass in a url, set of visited lists,
       and the word dictionary for keeping track of counted words"""

    # how to pass immutable data structures into a recursive function
    # initializes them is they are set to None
    if visited is None:
        visited = set()
    if word_dict is None:
        word_dict = {}

    # keep track of already visited urls
    visited.add(url)

    # analyze() returns a list of hyperlink URLs in web page url
    links, word_dict = analyze(url, word_dict)

    # recursively continue crawl from every link in links
    for link in links:
        # follow link only if not visited, within url scope, not a duplicate page
        # not a course eval page, and doesn't contain a space character
        if link not in visited and link[:27] == base_url and ' ' not in link \
                and dup_page not in link and download not in link:
            try:
                # recursively call this function, passing in visited url list and word dictionary
                webCrawl(link, visited, word_dict)
            except:
                pass

    # return word dictionary to caller
    return word_dict


def analyzeStats(url, word_dict):
    """function for getting urls and data contained on a webpage"""

    print('\n\nVisiting', url)  # for testing

    # obtain links in the web page
    content = urlopen(url).read().decode().lower()

    # initalize Collector class with url, feed in content of page
    collector = Collector(url)
    collector.feed(content)

    # get back the urls into a list
    urls = collector.getLinks()

    # get data in a list of (start_tag, data) tuples
    data_tup_list = collector.getData()

    # calculate frequency of words on page and return a word dictionary
    word_dict = frequency(data_tup_list, word_dict)

    # helper code for printing top 10 words as they exist in the current dicitonary
    #    helper = sorted(word_dict.items(), key = lambda x: x[1], reverse=True)
    #    print('\nTop 10' )
    #    for top in helper[:10]:
    #        print(top)

    # return the url list and the word dictionary
    return urls, word_dict


# set first url
url = 'https://www.cdm.depaul.edu/'
