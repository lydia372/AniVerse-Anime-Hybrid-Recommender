#!/usr/bin/env python3
import argparse
import csv
import os
import re
from zipfile import ZipFile
from bs4 import BeautifulSoup

REVIEW_BLOCK_SELECTOR = "div.borderDark"


def iter_review_blocks(soup):
    for block in soup.select(REVIEW_BLOCK_SELECTOR):
        if "Overall Rating" in block.get_text(" ", strip=True):
            yield block


def safe_text(node):
    return node.get_text(" ", strip=True) if node else ""


def parse_review(block):
    # permalink contains review id
    review_url = ""
    review_id = ""
    link = block.select_one('a[href*="/reviews.php?id="]')
    if link and link.get("href"):
        review_url = link["href"].strip()
        m = re.search(r"id=(\d+)", review_url)
        if m:
            review_id = m.group(1)

    # header section
    header = block.find("div", class_="spaceit")
    date = ""
    episodes_seen = ""
    overall_rating = ""
    helpful_count = ""
    username = ""
    profile_url = ""

    if header:
        # right column: date and overall rating
        right = header.find("div", class_="mb8")
        if right:
            # date
            date_div = right.find("div", title=True)
            if date_div:
                date = safe_text(date_div)
            # episodes seen line
            eps_div = right.find("div", class_="lightLink")
            if eps_div:
                episodes_seen = safe_text(eps_div)
            # overall rating number
            # structure: <a>Overall Rating</a> : 10
            overall_text = right.get_text(" ", strip=True)
            # try to extract trailing rating number
            m = re.search(r"Overall Rating\s*:?\s*(\d+)", overall_text)
            if m:
                overall_rating = m.group(1)

        # left column: profile + helpful count
        profile_link = header.select_one('a[href*="/profile/"]')
        if profile_link:
            profile_url = profile_link.get("href", "").strip()
            # username often in the second /profile link with text
            for l in header.select('a[href*="/profile/"]'):
                if l.get_text(strip=True):
                    username = l.get_text(strip=True)
                    break
        help_span = header.select_one('span[id^="rhelp"]')
        if help_span:
            helpful_count = safe_text(help_span)

    # review body and score table
    body = block.find("div", class_=lambda c: c and "textReadability" in c)
    scores = {}
    review_text = ""
    if body:
        # score table
        score_table = body.find("table")
        if score_table:
            for tr in score_table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    label = safe_text(tds[0])
                    value = safe_text(tds[1])
                    if label:
                        scores[label] = value
            score_table.decompose()
        # remove any leftover small blocks
        review_text = body.get_text("\n", strip=True)

    return {
        "review_id": review_id,
        "review_url": review_url,
        "username": username,
        "profile_url": profile_url,
        "date": date,
        "episodes_seen": episodes_seen,
        "overall_rating": overall_rating,
        "helpful_count": helpful_count,
        "review_text": review_text,
        "scores": scores,
    }


def iter_review_pages(zipf):
    names = [n for n in zipf.namelist() if n.startswith("reviews_") and n.endswith(".html")]
    # sort by page number
    def page_num(n):
        m = re.search(r"reviews_(\d+)\.html", n)
        return int(m.group(1)) if m else 0
    for name in sorted(names, key=page_num):
        yield name


def extract_reviews_from_zip(zip_path):
    anime_id = os.path.splitext(os.path.basename(zip_path))[0]
    with ZipFile(zip_path) as z:
        for page_name in iter_review_pages(z):
            html = z.read(page_name).decode("utf-8", errors="ignore")
            soup = BeautifulSoup(html, "html.parser")
            for block in iter_review_blocks(soup):
                review = parse_review(block)
                review["anime_id"] = anime_id
                review["review_page"] = page_name
                yield review


def main():
    ap = argparse.ArgumentParser(description="Extract MAL reviews from html zip files")
    ap.add_argument("--html-dir", default="data/html", help="Directory containing <anime_id>.zip")
    ap.add_argument("--out", default="data/reviews.csv", help="Output CSV file")
    ap.add_argument("--append", action="store_true", help="Append to existing CSV")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of zips to process")
    args = ap.parse_args()

    zip_files = [
        os.path.join(args.html_dir, f)
        for f in os.listdir(args.html_dir)
        if f.endswith(".zip")
    ]
    zip_files.sort(key=lambda p: int(os.path.splitext(os.path.basename(p))[0]) if os.path.splitext(os.path.basename(p))[0].isdigit() else 0)
    if args.limit:
        zip_files = zip_files[: args.limit]

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    fieldnames = [
        "anime_id",
        "review_id",
        "review_url",
        "username",
        "profile_url",
        "date",
        "episodes_seen",
        "overall_rating",
        "helpful_count",
        "score_overall",
        "score_story",
        "score_animation",
        "score_sound",
        "score_character",
        "score_enjoyment",
        "review_text",
        "review_page",
    ]

    mode = "a" if args.append else "w"
    with open(args.out, mode, encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not args.append:
            writer.writeheader()

        for i, zip_path in enumerate(zip_files, 1):
            for review in extract_reviews_from_zip(zip_path):
                scores = {k.lower(): v for k, v in review.get("scores", {}).items()}
                row = {
                    "anime_id": review.get("anime_id", ""),
                    "review_id": review.get("review_id", ""),
                    "review_url": review.get("review_url", ""),
                    "username": review.get("username", ""),
                    "profile_url": review.get("profile_url", ""),
                    "date": review.get("date", ""),
                    "episodes_seen": review.get("episodes_seen", ""),
                    "overall_rating": review.get("overall_rating", ""),
                    "helpful_count": review.get("helpful_count", ""),
                    "score_overall": scores.get("overall", ""),
                    "score_story": scores.get("story", ""),
                    "score_animation": scores.get("animation", ""),
                    "score_sound": scores.get("sound", ""),
                    "score_character": scores.get("character", ""),
                    "score_enjoyment": scores.get("enjoyment", ""),
                    "review_text": review.get("review_text", ""),
                    "review_page": review.get("review_page", ""),
                }
                writer.writerow(row)

            if i % 100 == 0:
                print(f"Processed {i}/{len(zip_files)} anime zips")


if __name__ == "__main__":
    main()
