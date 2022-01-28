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
        self.most_outlinks = ("", 0)  # (updated in start_crawling) key
        self.downloaded_URLs = {}  # (updated in is_valid) key: URL, value: bool - 1 if trap, 0 if not
        self.longest_page = ("", 0)  # (updated in extract_next_links) 0 is URL, 1 is word count (DONE)
        self.corpus_word_freq = {}  # (updated in extract_next_links)

        self.stopwords = []
        with open('stopwords.txt') as file:
            for line in file:
                self.stopwords.append(line.rstrip())

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

            # keep track of valid_outlinks for current url
            valid_outlinks = 0
            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    # increment valid_outlinks for current url
                    valid_outlinks += 1
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

            # update max out link
            if valid_outlinks > self.most_outlinks[1]:
                if url_data['is_redirected']:
                    self.most_outlinks = (url_data['final_url'], valid_outlinks)
                else:
                    self.most_outlinks = (url_data['url'], valid_outlinks)

        self.write_analytics_file()

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
            # if url ends with any of these, no need to crawl
            str_url = str(url_data['url'])
            if str_url.endswith('jpg') or str_url.endswith('png') or str_url.endswith('jpeg') or str_url.endswith('gif'):
                return []

            # if content is None, http_code is 404, or size == 0
            if url_data['content'] is None or url_data['http_code'] == 404 or url_data["size"] == 0:
                return []

            doc = html.fromstring(url_data['content'])
            page_text = BeautifulSoup(url_data['content'], 'html.parser').get_text()
            token_list = PartA.tokenize(page_text)
            page_length = len(token_list)

            # get longest page
            if page_length > self.longest_page[1]:
                if url_data['is_redirected']:
                    self.longest_page = (url_data['final_url'], page_length)
                else:
                    self.longest_page = (url_data['url'], page_length)

            page_word_freq = PartA.compute_word_frequencies(token_list)

            if url_data['is_redirected']:
                doc = html.make_links_absolute(doc, base_url=url_data['final_url'])
            else:
                doc = html.make_links_absolute(doc, base_url=url_data['url'])

            for link in doc.xpath('//a/@href'):
                outputLinks.append(link)

            # URLs per subdomain
            if url_data['is_redirected']:
                subd = tldextract.extract(url_data['final_url'])[0]
            else:
                subd = tldextract.extract(url_data['url'])[0]
            if subd in self.subdomains:
                self.subdomains[subd] += 1
            else:
                self.subdomains[subd] = 1

            # add page's words to corpus frequency count
            for key in page_word_freq:
                if key not in self.stopwords and (len(key) > 1 or (str.isdigit(key) and len(key) > 2)):
                    if key in self.corpus_word_freq:
                        self.corpus_word_freq[key] += page_word_freq[key]
                    else:
                        self.corpus_word_freq[key] = page_word_freq[key]
        
            # self.write_analytics_file()

        except etree.ParserError:
            print('XML is empty or invalid')
        except ValueError:
            print('Encoding Error')

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
            # add new url key to downloaded_URLs dictionary
            self.downloaded_URLs[url] = 0

            # check long messy strings:
            if len(url) > 100:
                # update self.traps
                self.downloaded_URLs[url] = 1
                return False


            # TODO: [Done] Calendar - look for frequencies of words in a word-set (also queries)
            # TODO: [Done] jpg, png, img, or other image extensions
            # TODO: [Done] if http_code == 404
            # TODO: [Done] if doesnt exist: if content, content_empty is None or size == 0
            # TODO: [Done] ask if compute_word is sorted, we dont want to do O(n) everytime
            # TODO: [Done] move outputLinks above to only add VALID outputLinks
            # TODO: www is not a subdomain

            # contains fragment
            if "#" in url:
                # update self.traps
                self.downloaded_URLs[url] = 1
                return False

            # keywords specific traps in query:
            if "jession" in parsed.query or "sessid" in parsed.query or "sid" in parsed.query\
                    or "day" in parsed.query or "date" in parsed.query or "week" in parsed.query \
                    or "month" in parsed.query or "year" in parsed.query:
                # update self.traps
                self.downloaded_URLs[url] = 1
                return False

            # Repeating Directories
            token_list = PartA.tokenize(parsed.path)
            token_dict = PartA.compute_word_frequencies(token_list)
            for key, freq in token_dict.items():
                if freq >= 3:
                    # update self.traps
                    self.downloaded_URLs[url] = 1
                    return False
                else:
                    break

            # Calendar
            url_token = PartA.tokenize(url)
            cal = ["calendar", "week", "month", "year", "event", "date", "dates", "day", "today"]
            months = ["january", "jan",
                      "february", "feb",
                      "march", "mar",
                      "april", "apr",
                      "may",
                      "june", "jun",
                      "july", "jul",
                      "august", "aug",
                      "september", "sept",
                      "october", "oct",
                      "november", "nov",
                      "december", "dec"]
            cal_tokens = 0
            for token in url_token:
                if token in cal or token in months:
                    cal_tokens += 1
                if cal_tokens >= 3:
                    return False

            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())
        except TypeError:
            print("TypeError for ", parsed)
            return False

    def write_analytics_file(self):
        with open('analytics.txt', 'w', encoding="utf-8") as analytics:
            file_content = 'List of Subdomains with Number of URLs\n'
            for sub in self.subdomains:
                file_content += sub + '\t' + str(self.subdomains[sub]) + '\n'
            file_content += ('\nPage with Most Valid Outlinks\n' 
                + str(self.most_outlinks[0]) + '\t' + str(self.most_outlinks[1]) + '\n')
            file_content += ('\nLongest Page by Words\n'
                + str(self.longest_page[0]) + '\t' + str(self.longest_page[1]) + '\n'
                + '\nDownloaded URLs and Traps (1 if trap, 0 if not)\n')
            for url in self.downloaded_URLs:
                file_content += url + '\t' + str(self.downloaded_URLs[url]) + '\n'
            file_content += '\n50 Most Common Words (Excluding Stop Words)\n'
            # get 50 most common words
            most_common = dict(sorted(self.corpus_word_freq.items(), key = itemgetter(1), reverse = True)[:50])
            for word in most_common:
                file_content += word + '\t' + str(most_common[word]) + '\n' 
            analytics.write(file_content)
