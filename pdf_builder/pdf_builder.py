
import copy
import csv
from ctypes import alignment

from reportlab.pdfgen.canvas import Canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph, SimpleDocTemplate, PageBreak
from reportlab.rl_config import canvas_basefontname as _baseFontName

# The books of the bible
BOOKS = [
	# The Five Books
	"Genesis",
	"Exodus",
	"Leviticus",
	"Numbers",
	"Deuteronomy",
	# The History
	"Joshua",
	"Judges",
	"Ruth",
	"ISamuel",
	"IISamuel",
	"IKings",
	"IIKings",
	"IChronicles",
	"IIChronicles",
	"Ezra",
	"Nehemiah",
	"Esther",
	"Job",
	"Psalms",
	"Proverbs",
	"Ecclesiastes",
	"Song_of_Solomon",
	"Isaiah",
	"Jeremiah",
	"Lamentations",
	"Ezekiel",
	"Daniel",
	# The Minor Prophets
	"Hosea",
	"Joel",
	"Amos",
	"Obadiah",
	"Jonah",
	"Micah",
	"Nahum",
	"Habakkuk",
	"Zephaniah",
	"Haggai",
	"Zechariah",
	"Malachi",
	# The Four Gospels
	"Matthew",
	"Mark",
	"Luke",
	"John",
	# Acts
	"Acts",
	# The Epistles
	"Romans",
	"ICorinthians",
	"IICorinthians",
	"Galatians",
	"Ephesians",
	"Philippians",
	"Colossians",
	"IThessalonians",
	"IIThessalonians",
	"ITimothy",
	"IITimothy",
	"Titus",
	"Philemon",
	"Hebrews",
	"James",
	"IPeter",
	"IIPeter",
	"IJohn",
	"IIJohn",
	"IIIJohn",
	"Jude",
	# Revelation
	"Revelation",
]

PAGEWIDTH, PAGEHEIGHT = letter
PAGEINCH = 72
PAGECENTER = (PAGEWIDTH/2, PAGEHEIGHT/2)
PAGEMARGIN = PAGEINCH

STYLES = getSampleStyleSheet()
STYLENORM = STYLES['Normal']
STYLENORMCENTER = ParagraphStyle(name='Normal',
		fontName=_baseFontName,
		fontSize=10,
		leading=12,
		alignment=TA_CENTER,
)
STYLEHEAD1 = STYLES['Heading1']
STYLEHEAD2 = STYLES['Heading2']
STYLEHEAD3 = STYLES['Heading3']

bookcurr = -1
chaptercurr = -1
chaptersbookmarked = []
chapterindexes = []
chaptercounts = [] # An array containing how many chapters are in each book


def draw_chapter_index_page(book:int):
	parts = []
	parts.append(PageBreak())
	parts.append(
			Paragraph(
				"{book}<a name='ChapterIndex{book}'/>".format(book=BOOKS[book]),
				STYLEHEAD1))

	for chapter in range(chaptercounts[book]):
		parts.append(Paragraph(
				"<a href=#{book}{chp} color=blue>{chp}</a>".format(
					chp=chapter+1, 
					book=BOOKS[book],
					),
				STYLEHEAD3))
				
	parts.append(PageBreak())

	return parts


def chapter_counts(csvtext):
	countslist = []
	bookcurr = -1
	chaptercurr = -1
	chaptercount = 0
	spamreader = csv.reader(csvtext, delimiter=",", quotechar='"')
	for row in spamreader:
		book, chapter, verse, text = row
		book = int(book)-1
		chapter = int(chapter)

		if bookcurr != book:
			chaptercount = 0
			bookcurr = book
			countslist.append(0)

		if chaptercurr != chapter:
			chaptercurr = chapter
			chaptercount += 1

		countslist[bookcurr] = chaptercount

	return countslist


def draw_book(csvtext):
	generator = verse_gen(csvtext)

	i = 0
	bookcurr = -1
	chaptercurr = -1
	parts = []
	while True:
		try:
			book, chapter, verse, text = next(generator)
			bookindex = int(book)-1

			parts.append(Paragraph("", STYLEHEAD1))

			# Book
			if bookcurr != bookindex:
				bookcurr = bookindex
				chaptercurr = -1
				print( BOOKS[bookcurr] )
				parts += draw_chapter_index_page(bookcurr)
				parts.append(Paragraph("", STYLEHEAD1))
				
			# Chapter
			if chaptercurr != chapter:
				chaptercurr = chapter
				parts.append(
					Paragraph(
						"Chapter {chapter}<a name='{book}{chapter}'/>".format(
								chapter=chaptercurr, book=BOOKS[bookcurr]),
						STYLEHEAD3
					)
				)
				parts.append(Paragraph("", STYLEHEAD1))

			# Verse
			parts.append(
				Paragraph(
					"{verse} {text}".format(verse=verse, text=text),
					STYLENORM
				) 
			)
			parts.append(Paragraph("", STYLEHEAD1))

		except StopIteration:
			generator.close()
			break

		i += 1
	
	print("Building ...")
	summaryName = SimpleDocTemplate("pdf_builder/hello.pdf")
	summaryName._doSave = False
	summaryName.build(
			parts, onFirstPage=myOnFirstPage, 
			onLaterPages=myOnLaterPages)
	summaryName.canv.save()

	print("Done!")


def myOnFirstPage(canvas, doc):
	col = 0
	row = 0
	i = 0
	while i < len(BOOKS):
		col = i % 3
		if col == 0:
			row += 1
		
		centerx, _ = PAGECENTER

		posx = 0
		posy = row * -20

		if col == 0:
			posx = -200
		elif col == 1:
			posx = -0
		elif col == 2:
			posx = 200
		
		posx += centerx
		posy += 650

		p = Paragraph(
				"""<div alignment"='center'>
				<a name='BookIndex'/>
				<a href=#ChapterIndex{bk} color=blue>{bk}</a>
				</div>
				"""
				.format(bk=BOOKS[i]),
				style=STYLENORMCENTER)
		p.wrap(100,100)
		p.drawOn(canvas, posx, posy)

		i += 1


def myOnLaterPages(canvas, doc):
	p = Paragraph(
			"""<a href=#BookIndex color=blue>Book Index</a>""",
			style=STYLENORMCENTER)
	p.wrap(PAGEWIDTH,0)
	p.drawOn(canvas, 0, PAGEHEIGHT-0)


def verse_gen(csvtxt):
	"""A generator looping through every verse in the bible."""
	spamreader = csv.reader(csvtxt, delimiter=",", quotechar='"')
	i = 0
	for row in spamreader:
		book, chapter, verse, text = row
		yield row
	return


def main():
	global chaptercounts

	# Load csv
	with open("./csv/AMP_fixed.csv") as csvtext:
		chaptercounts = chapter_counts(csvtext)
	with open("./csv/AMP_fixed.csv") as csvtext:
		draw_book(csvtext)


if __name__ == "__main__":
	main()