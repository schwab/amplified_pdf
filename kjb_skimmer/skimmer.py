#!/usr/bin/env python3

import argparse
import json
import string
from time import sleep
from bs4 import BeautifulSoup
import urllib.request

SITE = "https://www.kingjamesbibleonline.org/{path}"
VERSEPATH = "Bible-Verses-{a}"
USER_AGENT = "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7"
HEADERS = {"User-Agent": USER_AGENT}

def scrape(requests_per_second:int):
	"""Scrapes the King James Bible Online website keyword-verse pairs and saves
	to file.
	
	:param requests_per_second: The requests per second limit."""
	items = {}

	# Scrape topics by letter
	verselist_paths = []
	for letter in string.ascii_uppercase:
		print(f"Scraping letter: {letter}")
		sleep(1.0/float(requests_per_second))

		url = SITE.format(path=VERSEPATH.format(a=letter))
		text = urllib.request.urlopen(
			urllib.request.Request(url, None, HEADERS)
		).read()
		doc = BeautifulSoup(text, "html.parser")
		
		for col in doc.table.find_all("td"):
			for line in col.find_all("p"):
				try:
					verselist_paths.append((line.strong.text, line.a["href"]))
				except AttributeError:
					pass
	
	# Scrape topics
	for p in verselist_paths:
		print("Scraping tag:", p[0])
		sleep(1.0/float(requests_per_second))
		items[p[0]] = []

		url = SITE.format(path=p[1][3:])
		try:
			text = urllib.request.urlopen(
				urllib.request.Request(url, None, HEADERS)
			).read()
		except:
			continue
		versedoc = BeautifulSoup(text, "html.parser")
		for vrs_line in versedoc.find_all("span", attrs={"itemprop":"hasPart"}):
			items[p[0]].append(vrs_line.strong.a["title"])

	# Save to file
	with open("kjb_skimmer_output.txt", "w") as f:
		f.write(json.dumps(items, indent=2, sort_keys=True))


if __name__ == "__main__":
	rps = 3

	p = argparse.ArgumentParser()
	p.add_argument(
		"--rps", "-r", help="How frequesntly to make http requests",
		type=int,
		default=rps,
	)
	args = p.parse_args()
	if args.rps:
		rps = args.rps
	
	scrape(rps)



