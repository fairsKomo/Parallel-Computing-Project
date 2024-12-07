import csv
import time
from jobspy import scrape_jobs


# Function to benchmark scraping jobs from a website
def benchmark_scraping(
    site_name, search_term, location, results_wanted, hours_old, country_code
):
    start_time = time.time()
    jobs = scrape_jobs(
        site_name=[site_name],
        search_term=search_term,
        location=location,
        results_wanted=results_wanted,
        hours_old=hours_old,
        country_indeed=country_code,
    )
    duration = time.time() - start_time
    print(f"{site_name.capitalize()} scraping took {duration:.2f} seconds")
    return jobs, duration


# Save scraped jobs to CSV
def save_to_csv(jobs, filename):
    jobs.to_csv(filename, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)


def main():
    # Parameters for job scraping
    search_term = "software engineer"
    location = "New York, NY"
    results_wanted = 20
    hours_old = 72
    country_code = "usa"

    # Start measuring total time
    start_time_total = time.time()

    # Benchmark scraping for different job sites
    google_jobs, google_duration = benchmark_scraping(
        site_name="google",
        search_term=search_term,
        location=location,
        results_wanted=results_wanted,
        hours_old=hours_old,
        country_code=country_code,
    )

    indeed_jobs, indeed_duration = benchmark_scraping(
        site_name="indeed",
        search_term=search_term,
        location=location,
        results_wanted=results_wanted,
        hours_old=hours_old,
        country_code=country_code,
    )

    glassdoor_jobs, glassdoor_duration = benchmark_scraping(
        site_name="glassdoor",
        search_term=search_term,
        location=location,
        results_wanted=results_wanted,
        hours_old=hours_old,
        country_code=country_code,
    )

    zip_recruiter_jobs, zip_recruiter_duration = benchmark_scraping(
        site_name="zip_recruiter",
        search_term=search_term,
        location=location,
        results_wanted=results_wanted,
        hours_old=hours_old,
        country_code=country_code,
    )

    # Total time for all scraping
    total_duration = time.time() - start_time_total
    print(f"Total scraping process took {total_duration:.2f} seconds")

    # Save results to CSV
    save_to_csv(google_jobs, "google_jobs.csv")
    save_to_csv(indeed_jobs, "indeed_jobs.csv")
    save_to_csv(glassdoor_jobs, "glassdoor_jobs.csv")
    save_to_csv(zip_recruiter_jobs, "zip_recruiter_jobs.csv")


if __name__ == "__main__":
    main()
