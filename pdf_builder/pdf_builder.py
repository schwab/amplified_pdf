
import copy
import csv
from ctypes import alignment

from reportlab.pdfgen.canvas import Canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph, SimpleDocTemplate, PageBreak, Spacer
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
	"I Samuel",
	"II Samuel",
	"I Kings",
	"II Kings",
	"I Chronicles",
	"II Chronicles",
	"Ezra",
	"Nehemiah",
	"Esther",
	"Job",
	"Psalms",
	"Proverbs",
	"Ecclesiastes",
	"Song of Solomon",
	"I saiah",
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
	"I Corinthians",
	"II Corinthians",
	"Galatians",
	"Ephesians",
	"Philippians",
	"Colossians",
	"I Thessalonians",
	"II Thessalonians",
	"I Timothy",
	"II Timothy",
	"Titus",
	"Philemon",
	"Hebrews",
	"James",
	"I Peter",
	"II Peter",
	"I John",
	"II John",
	"III John",
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
STYLETITLE = STYLES['Title']
STYLETITLECENTER = ParagraphStyle(
		name='TitleCenter',
		parent=STYLETITLE,
		alignment=TA_CENTER,
)
STYLEHEAD1 = STYLES['Heading1']
STYLEHEAD1CENTER = ParagraphStyle(
		name='Heading1Center',
		parent=STYLEHEAD1,
		alignment=TA_CENTER,
)
STYLEHEAD2 = STYLES['Heading2']
STYLEHEAD2CENTER = ParagraphStyle(
		name='Heading2Center',
		parent=STYLEHEAD2,
		alignment=TA_CENTER,
)
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
				"{book}<a name='ChapterIndex{bookid}'/>".format(
					book=BOOKS[book],
					bookid=BOOKS[book].replace(" ", ""),
					),
				STYLETITLECENTER))
	parts.append(Spacer(0,20))

	text = ""
	paragraphs = []
	added = 0
	for chapter in range(chaptercounts[book]):
		text += "<a href=#{bookid}{chp} color=blue>{chp}</a>".format(
				chp=chapter+1, 
				book=BOOKS[book],
				bookid=BOOKS[book].replace(" ", ""),
				)
		added += 1

		if added == 5:
			p = Paragraph(text, STYLEHEAD1CENTER)
			p.wrap(PAGEWIDTH,0)
			paragraphs.append(p)

			added = 0
			text = ""

		elif chapter < chaptercounts[book]-1:
			text += ",	"

	parts += paragraphs


	p = Paragraph(text, STYLEHEAD1CENTER)
	p.wrap(PAGEWIDTH,0)
	parts.append(p)	

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
						"{book} {chapter}<a name='{bookid}{chapter}'/>".format(
								chapter=chaptercurr, book=BOOKS[bookcurr],
								bookid=BOOKS[bookcurr].replace(" ", ""),
								),
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
	# Title
	p = Paragraph("""Bible Index
			<a name='BookIndex'/>""",
			style=STYLETITLECENTER)
	p.wrap(PAGEWIDTH,0)
	p.drawOn(canvas, 0, PAGEHEIGHT-50)

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

		p = Paragraph("""
				<a href=#ChapterIndex{bookid} color=blue>{bk}</a>
				"""
				.format(
					bk=BOOKS[i],
					bookid=BOOKS[i].replace(" ", ""),
					),
				style=STYLENORMCENTER)
		p.wrap(100,100)
		p.drawOn(canvas, posx-50, posy)

		i += 1


def myOnLaterPages(canvas, doc):
	p = Paragraph(
			"""<a href=#BookIndex color=blue>Book Index</a>""",
			style=STYLENORMCENTER)
	p.wrap(100,0)
	p.drawOn(canvas, PAGEWIDTH/2 - 50, PAGEHEIGHT-0)


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