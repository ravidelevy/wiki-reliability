from crawler import crawl, save


def main():
    save(crawl("data", number_of_records=1000), "pages")

if __name__ == '__main__':
    main()
