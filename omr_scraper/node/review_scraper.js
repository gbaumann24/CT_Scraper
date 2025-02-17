#!/usr/bin/env node

const fs = require('fs');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cheerio = require('cheerio');
const csvParser = require('csv-parser');
const cliProgress = require('cli-progress');

// Enable stealth mode
puppeteer.use(StealthPlugin());

// --------------------------
// CONFIGURATION & FILES
// --------------------------
const HEARTBEAT_FILE = 'heartbeat_reviews.txt';
const PROGRESS_FILE = 'progress_reviews.txt';
const INPUT_CSV = 'capterra_products_A_Z_matched_first_half.csv';
const OUTPUT_CSV = 'capterra_reviews_matched.csv';

const fieldnames = [
	'ToolName',
	'Category',
	'Product Link',
	'ReviewerName',
	'Role',
	'Country',
	'Industry',
	'Employee Count',
	'Use Duration',
	'Rating',
	'Review Date',
	'Comment',
	'Advantages',
	'Disadvantages',
];

// --------------------------
// UTILITY FUNCTIONS
// --------------------------
function updateHeartbeat(productIndex) {
	const content = `${new Date().toISOString()} - Product Index: ${productIndex}\n`;
	fs.writeFileSync(HEARTBEAT_FILE, content, 'utf8');
}

function updateProgress(productIndex) {
	fs.writeFileSync(PROGRESS_FILE, String(productIndex), 'utf8');
}

function getStartIndex() {
	if (fs.existsSync(PROGRESS_FILE)) {
		try {
			const content = fs.readFileSync(PROGRESS_FILE, 'utf8').trim();
			const index = parseInt(content, 10);
			return isNaN(index) ? 0 : index + 1; // resume from next product
		} catch (err) {
			console.error('Error reading progress file:', err);
		}
	}
	return 0;
}

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

// New helper function to determine if a review is older than two years.
// Reviews exactly "vor zwei Jahren" are allowed.
function isReviewOlderThan2Years(reviewDate) {
	if (!reviewDate) return false;
	// Check if the review date mentions years
	const yearsMatch = reviewDate.match(/vor\s+(\d+)\s+Jahren?/i);
	if (yearsMatch) {
		const years = parseInt(yearsMatch[1], 10);
		// Only reviews with more than 2 years are too old.
		return years > 2;
	}
	// Check if the review date mentions months (convert months to years)
	const monthsMatch = reviewDate.match(/vor\s+(\d+)\s+Monaten/i);
	if (monthsMatch) {
		const months = parseInt(monthsMatch[1], 10);
		// More than 24 months (i.e. older than 2 years) are too old.
		return months > 24;
	}
	return false;
}

// --------------------------
// BROWSER & PAGE SETUP
// --------------------------
async function getDriver() {
	const userAgents = [
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
	];
	const launchOptions = {
		headless: true,
		args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu', '--lang=en-US', '--window-size=1280,720', '--incognito', '--disable-blink-features=AutomationControlled'],
	};
	const browser = await puppeteer.launch(launchOptions);
	const page = await browser.newPage();
	// Set a random User-Agent & Accept-Language header.
	const ua = userAgents[Math.floor(Math.random() * userAgents.length)];
	await page.setUserAgent(ua);
	await page.setExtraHTTPHeaders({
		'Accept-Language': 'en-US,en;q=0.9',
	});
	await sleep(2000);
	return { browser, page };
}

// --------------------------
// SIMULATE HUMAN INTERACTION
// --------------------------
async function simulateHumanInteraction(page) {
	const iterations = Math.floor(Math.random() * 4) + 3;
	for (let i = 0; i < iterations; i++) {
		const waitTime = 500 + Math.random() * 2000;
		const scrollDistance = 100 + Math.random() * 200;
		const direction = Math.random() < 0.8 ? 1 : -1;
		await page.evaluate((distance, direction) => window.scrollBy(0, distance * direction), scrollDistance, direction);
		await sleep(waitTime);
	}
	// Finally, scroll to the bottom.
	await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
	await sleep(4000 + Math.random() * 4000);
}

// --------------------------
// FETCH PAGE WITH RETRIES
// --------------------------
async function fetchPage(url, page, maxRetries = 3) {
	let attempt = 1;
	while (attempt <= maxRetries) {
		try {
			console.log(`Fetching URL: ${url} (Attempt ${attempt})`);
			await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
			// Dismiss cookie popover if present.
			try {
				const cookieSelector = '#onetrust-accept-btn-handler';
				await page.waitForSelector(cookieSelector, { timeout: 5000 });
				console.log('Cookie popover detected. Dismissing it.');
				await page.click(cookieSelector);
				await sleep(2000);
			} catch (err) {
				// No cookie popover
			}
			await simulateHumanInteraction(page);
			return await page.content();
		} catch (err) {
			console.error(`Error fetching ${url} on attempt ${attempt}: ${err.message}`);
			await sleep(attempt * 5000);
			attempt++;
		}
	}
	return null;
}

// --------------------------
// DATA EXTRACTION
// --------------------------
function extractReviews(pageContent) {
	const $ = cheerio.load(pageContent);
	const reviewContainer = $('#reviews');
	if (!reviewContainer.length) {
		console.log('No review container found on the page.');
		return [];
	}
	const reviews = [];
	reviewContainer.children().each((idx, el) => {
		const reviewData = {};
		const reviewEl = $(el);

		reviewData['ReviewerName'] = reviewEl.find('.h5.fw-bold.mb-2').first().text().trim() || '';
		reviewData['Raw Role'] = reviewEl.find('.text-ash.mb-2').first().text().trim() || '';

		const infoContainer = reviewEl.find('div.col-12.col-md-6.col-lg-12.pt-3.pt-md-0.pt-lg-3.text-ash').first();
		let rawIE = '';
		let useDuration = '';
		if (infoContainer.length) {
			const children = infoContainer.children().filter((i, child) => $(child).get(0).tagName);
			if (children.length >= 1) {
				rawIE = $(children[0]).text().trim();
			}
			if (children.length >= 2) {
				useDuration = $(children[1]).text().trim().replace('Verwendete die Software für:', '').trim();
			}
		}
		reviewData['Raw Industry & Employee'] = rawIE;
		reviewData['Use Duration'] = useDuration;

		let rating = '';
		const starsWrapper = reviewEl.find('span.stars-wrapper').first();
		if (starsWrapper.length) {
			const ratingSpan = starsWrapper.next('span.ms-1').first();
			if (ratingSpan.length) {
				rating = ratingSpan.text().trim();
			}
		}
		reviewData['Rating'] = rating;

		let reviewDate = '';
		const starRatingComponent = reviewEl.find('span.star-rating-component--primary').first();
		if (starRatingComponent.length) {
			const reviewDateSpan = starRatingComponent.next('span.ms-2').first();
			if (reviewDateSpan.length) {
				reviewDate = reviewDateSpan.text().trim();
			}
		}
		reviewData['Review Date'] = reviewDate;

		let comment = '';
		let advantages = '';
		let disadvantages = '';
		reviewEl.find('p').each((i, pElem) => {
			const pText = $(pElem).text().trim();
			if (pText.includes('Kommentare:')) {
				comment = pText.replace('Kommentare:', '').trim();
			} else if (pText.includes('Vorteile:')) {
				const nextElem = reviewEl.find('p').eq(i + 1);
				if (nextElem.length) {
					advantages = nextElem.text().trim();
				}
			} else if (pText.includes('Nachteile:')) {
				const nextElem = reviewEl.find('p').eq(i + 1);
				if (nextElem.length) {
					disadvantages = nextElem.text().trim();
				}
			}
		});
		reviewData['Comment'] = comment;
		reviewData['Advantages'] = advantages;
		reviewData['Disadvantages'] = disadvantages;

		reviews.push(reviewData);
	});
	return reviews;
}

function extractPaginationInfo(pageContent) {
	const $ = cheerio.load(pageContent);
	const paginationUl = $('ul.pagination').first();
	let maxPage = 1;
	if (paginationUl.length) {
		const liItems = paginationUl.find('li');
		if (liItems.length >= 2) {
			const secondLastLi = liItems.eq(liItems.length - 2);
			const text = secondLastLi.text().trim();
			const parsed = parseInt(text, 10);
			if (!isNaN(parsed)) {
				maxPage = parsed;
			}
		}
	}
	console.log(`Max page number: ${maxPage}`);
	return maxPage;
}

function parseRole(rawRole) {
	if (rawRole.includes(' in ')) {
		const parts = rawRole.split(' in ', 2);
		return { role: parts[0].trim(), country: parts[1].trim() };
	}
	return { role: rawRole.trim(), country: '' };
}

function parseIndustryEmployee(rawIE) {
	rawIE = rawIE.replace(/’/g, '');
	if (rawIE.includes(',')) {
		const parts = rawIE.split(',', 2);
		return {
			industry: parts[0].trim(),
			employee: parts[1].replace(/Mitarbeiter/g, '').trim(),
		};
	}
	return { industry: rawIE.trim(), employee: '' };
}

function sanitizeText(text) {
	if (!text) return '';
	return text
		.replace(/[\n\r\t]/g, ' ')
		.replace(/\s+/g, ' ')
		.trim();
}

// --------------------------
// CSV HANDLING
// --------------------------
function readCSV(filePath) {
	return new Promise((resolve, reject) => {
		const results = [];
		fs.createReadStream(filePath)
			.pipe(csvParser())
			.on('data', (data) => results.push(data))
			.on('end', () => resolve(results))
			.on('error', (err) => reject(err));
	});
}

function appendToCSV(filePath, row) {
	fs.appendFileSync(filePath, row + '\n', 'utf8');
}

// --------------------------
// MAIN FUNCTION
// --------------------------
async function main() {
	// 1) Parse --direction from CLI
	//    Usage: node script.js --direction forward  OR  node script.js --direction backward
	let direction = 'forward'; // default
	const dirIndex = process.argv.indexOf('--direction');
	if (dirIndex !== -1 && process.argv[dirIndex + 1]) {
		direction = process.argv[dirIndex + 1].toLowerCase();
	}

	// Read input CSV
	let products;
	try {
		products = await readCSV(INPUT_CSV);
	} catch (err) {
		console.error(`Error reading ${INPUT_CSV}: ${err}`);
		process.exit(1);
	}

	// 2) Determine starting point (only used in forward mode for this example)
	let startIndex = 0;
	if (direction === 'forward') {
		startIndex = getStartIndex();
		console.log(`Resuming from product index: ${startIndex}`);
	} else {
		// In backward mode, you could choose to ignore progress
		// or implement a "reverse" resume logic.
		startIndex = products.length - 1;
		console.log(`Starting from the last product index: ${startIndex}`);
	}

	// 3) Create or append to the output CSV
	if (!fs.existsSync(OUTPUT_CSV) || startIndex === 0) {
		fs.writeFileSync(OUTPUT_CSV, fieldnames.join(',') + '\n', 'utf8');
	}

	// Initialize Puppeteer
	const { browser, page } = await getDriver();

	// Setup a progress bar
	const progressBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);

	// 4) Prepare products to process
	let productsToProcess;
	if (direction === 'forward') {
		productsToProcess = products.slice(startIndex);
	} else {
		// For backward, slice from 0..startIndex and reverse that array
		productsToProcess = products.slice(0, startIndex + 1).reverse();
	}

	progressBar.start(productsToProcess.length, 0);

	// 5) The main loop
	for (let i = 0; i < productsToProcess.length; i++) {
		// Calculate the "real" index in the original array if needed
		// Forward: real index = startIndex + i
		// Backward: real index = (startIndex - i)
		const realIndex = direction === 'forward' ? startIndex + i : startIndex - i;

		updateHeartbeat(realIndex);

		const product = productsToProcess[i];
		const category = product['Category'];
		const productLink = product['Product Link'];

		console.log(`\nProcessing product link: ${productLink} (Category: ${category})`);

		try {
			// Fetch the main product page
			let pageContent = await fetchPage(productLink, page);
			if (!pageContent) {
				console.log(`Skipping ${productLink} due to fetch issues.`);
				continue;
			}

			// If the "Neueste Bewertungen" button exists, click it to sort by newest reviews.
			const newestReviewsSelector = 'label.form-check-label[for="opt_most_recent"]';
			if (await page.$(newestReviewsSelector)) {
				console.log("Clicking 'Neueste Bewertungen' button to sort reviews.");
				await page.click(newestReviewsSelector);
				await sleep(2000);
				// Refresh page content after sorting.
				pageContent = await page.content();
			}

			// Extract the tool name from <h1.h3.mb-1>
			const toolName = await page.evaluate(() => {
				const el = document.querySelector('h1.h3.mb-1');
				return el ? el.innerText.replace('Erfahrungen', '').trim() : '';
			});

			// --------------------------
			// REVIEW EXTRACTION WITH DATE CHECK
			// --------------------------
			let allReviews = [];
			let stopProduct = false;

			// Process first page reviews
			let reviewsFromPage = extractReviews(pageContent);
			for (const review of reviewsFromPage) {
				if (isReviewOlderThan2Years(review['Review Date'])) {
					console.log(`Encountered review older than two years ("${review['Review Date']}"). Skipping remaining reviews for this product.`);
					stopProduct = true;
					break;
				}
				allReviews.push(review);
			}

			// Check pagination only if we haven't hit an old review already.
			const maxPage = extractPaginationInfo(pageContent);
			if (!stopProduct && maxPage > 1) {
				for (let p = 2; p <= maxPage; p++) {
					const pageUrl = `${productLink}?page=${p}`;
					console.log(`Processing pagination page: ${pageUrl}`);
					const pagContent = await fetchPage(pageUrl, page);
					if (!pagContent) {
						console.log(`Skipping pagination page ${pageUrl} due to fetch issues.`);
						continue;
					}
					const pagReviews = extractReviews(pagContent);
					for (const review of pagReviews) {
						if (isReviewOlderThan2Years(review['Review Date'])) {
							console.log(`Encountered review older than two years ("${review['Review Date']}") on page ${p}. Stopping further review scraping for this product.`);
							stopProduct = true;
							break;
						}
						allReviews.push(review);
					}
					if (stopProduct) break;
					await sleep(Math.random() * (6000 - 3000) + 3000);
				}
			}

			// Process and write each review
			for (const review of allReviews) {
				review['ToolName'] = toolName;
				review['Category'] = category;
				review['Product Link'] = productLink;

				// Parse Role & Country
				const { role, country } = parseRole(review['Raw Role'] || '');
				review['Role'] = role;
				review['Country'] = country;
				delete review['Raw Role'];

				// Parse Industry & Employee
				const { industry, employee } = parseIndustryEmployee(review['Raw Industry & Employee'] || '');
				review['Industry'] = industry;
				review['Employee Count'] = employee;
				delete review['Raw Industry & Employee'];

				// Sanitize text
				for (const key in review) {
					if (typeof review[key] === 'string') {
						review[key] = sanitizeText(review[key]);
					}
				}

				// Build CSV row
				const row = fieldnames.map((field) => `"${(review[field] || '').replace(/"/g, '""')}"`).join(',');
				appendToCSV(OUTPUT_CSV, row);
			}

			// Update progress file only in forward mode (optional)
			if (direction === 'forward') {
				updateProgress(realIndex);
			}

			// Random sleep between products
			await sleep(Math.random() * (8000 - 5000) + 5000);
		} catch (err) {
			console.error(`Error processing product ${productLink}: ${err.message}`);
		}

		progressBar.update(i + 1);
	}

	progressBar.stop();
	await browser.close();
	console.log(`\nExtraction complete. Reviews saved to '${OUTPUT_CSV}'.`);
}
main().catch((err) => {
	console.error('Fatal error:', err);
});
