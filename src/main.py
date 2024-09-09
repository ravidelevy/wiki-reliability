from crawler import crawl, save


def main():
    save(crawl("data"), "pages")

if __name__ == '__main__':
    main()
