import logging
import re
import PartA
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import time
from operator import itemgetter

from lxml import etree
from lxml import html
import tldextract

logger = logging.getLogger(__name__)


class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus


        # analytics
        self.subdomains = {}  # (updated in extract_next_links) key: subdomain, value: number of URLs (DONE)
        self.most_outlinks = ("", 0)  # (updated in extract_next_links) key
        self.downloaded_URLs = {}  # (updated in is_valid) key: URL, value: bool - 1 if trap, 0 if not
        self.longest_page = ("", 0)  # (updated in extract_next_links) 0 is URL, 1 is word count (DONE)
        self.corpus_word_freq = {}  # (updated in extract_next_links)

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched,
                        len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.
        Suggested library: lxml
        """

        outputLinks = []

        try:
            doc = html.fromstring(url_data['content'])
            page_text = BeautifulSoup(url_data['content'], 'html.parser').get_text()
            token_list = PartA.tokenize(page_text)
            page_length = len(token_list)

            # get longest page
            if page_length > self.longest_page[1]:
                self.longest_page = (url_data['url'], page_length)

            page_word_freq = PartA.compute_word_frequencies(token_list)

            doc = html.make_links_absolute(doc, base_url=url_data['url'])

            for link in doc.xpath('//a/@href'):
                outputLinks.append(link)

            # URLs per subdomain
            subd = tldextract.extract(url_data['url'])[0]
            if subd in self.subdomains:
                self.subdomains[subd] += 1
            else:
                self.subdomains[subd] = 1

            # max valid outlinks
            if len(outputLinks) > self.most_outlinks[1]:
                self.most_outlinks = (url_data['url'], len(outputLinks))

            # add page's words to corpus frequency count
            for key in page_word_freq:
                if key in self.corpus_word_freq:
                    self.corpus_word_freq[key] += page_word_freq[key]
                else:
                    self.corpus_word_freq[key] = page_word_freq[key]
        
            self.write_analytics_file()

        except etree.ParserError:
            print('XML is empty or invalid')

        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:

            tokenlist = PartA.tokenize(parsed.path)
            tokendict = PartA.compute_word_frequencies(tokenlist)
            print(parsed)
            print(tokenlist)
            print(PartA.compute_word_frequencies(tokenlist))

            # check long messy strings:
            if len(parsed.path) > 100:
                print("Long Messy Link caught: " )
                #update self.traps
                #return False



            # Repeating directories:
            for key,freq in tokendict.items():
                if freq >= 3:
                    print("Repeating diretories caught: " + key)
                # update self.traps
                #return False

            # Extra directories:

            # has_cycle: check if Link A directs to Link B. Then Link B directs back to Link B
            #check downloaded_URL dictionary to see how many times it's visited
            #if visited 50 times, it's a trap


            # Calendar:

            # contains fragment
            if parsed.fragment in parsed:
                # update self.traps
                #return False

            #has_repeated_query_params
                querylist = PartA.tokenize(parsed.query)
                querydict = PartA.compute_word_frequencies(querylist)
                print(querylist)
                print(querydict)

                for key, value in querydict.items():
                    if value >= 2:
                        print(key)
                        # update self.traps
                        # return False



            #check_history

            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())
            # need to increment the number of 

        except TypeError:
            print("TypeError for ", parsed)
            return False

    def write_analytics_file(self):
        stop_words = []
        with open('stopwords.txt') as st:
            stop_words = PartA.tokenize(st.read())

        with open('analytics.txt', 'w') as analytics:
            file_content = 'List of Subdomains with Number of URLs\n'
            for sub in self.subdomains:
                file_content += sub + '\t' + str(self.subdomains[sub]) + '\n'
            file_content += ('\nPage with Most Valid Outlinks\n' 
                + str(self.most_outlinks[0]) + '\t' + str(self.most_outlinks[1])
                + '\nDownloaded URLs and Traps (1 if trap, 0 if not)\n'
                + '\n50 Most Common Words (Excluding Stop Words)\n')
            # remove stop words from dictionary
            for word in self.corpus_word_freq:
                if word in stop_words:
                    del self.corpus_word_freq[word]
            # get 50 most common words
            most_common = dict(sorted(self.corpus_word_freq.items(), key = itemgetter(1), reverse = True)[:50])
            for word in most_common:
                file_content += word + '\t' + str(most_common[word]) + '\n' 
            analytics.write(file_content)       
          

# #repeating direcotries
# test = Crawler
# test.is_valid(test,"https://www.google.com/maps/place/Amen+Acai/maps/maps/@33.8518045,-118.0091297,12z/data=!4m9!1m2!2m1!1samen+acai!3m5!1s0x80dcd7b4894c09c7:0x809dcd5c1f96c4df!8m2!3d33.8582532!4d-117.9216658!15sCglhbWVuIGFjYWlaCyIJYW1lbiBhY2FpkgEKanVpY2Vfc2hvcA")


# #repeating query parameter
# test.is_valid(test,"https://example.com/page?filter=red&filter=red")