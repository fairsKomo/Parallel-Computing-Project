import csv
import time
from jobspy import scrape_jobs
from concurrent.futures import ThreadPoolExecutor
import threading
import pandas as pd  # Import Pandas for DataFrame operations

# Global shared variables
total_scraped_jobs = 0
total_scraped_time = 0
total_jobs_saved = 0

# Lock object for critical section
lock = threading.Lock()

# Atomic-like operations using Semaphore
semaphore = threading.Semaphore(1)

# Function to benchmark scraping jobs from a website
def benchmark_scraping(site_name, search_term, location, results_wanted, hours_old, country_code, method="critical"):
    global total_scraped_jobs, total_scraped_time, total_jobs_saved  # Access global variables
    
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
        
    # Implementing different methods to avoid race conditions

    if method == "critical":
        # Critical Section: Locking the shared resources to prevent race conditions
        with lock:
            total_scraped_jobs += len(jobs)
            total_scraped_time += duration
            total_jobs_saved += len(jobs)

    elif method == "atomic":
        # Atomic-like: Using a semaphore to ensure exclusive access
        semaphore.acquire()
        total_scraped_jobs += len(jobs)
        total_scraped_time += duration
        total_jobs_saved += len(jobs)
        semaphore.release()

    elif method == "reduction":
        # Reduction: Instead of updating global variables, return the list of jobs and duration
        return jobs, duration

    return jobs, duration


# Save scraped jobs to CSV
def save_to_csv(jobs, filename):
    jobs.to_csv(filename, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)


def main():
    global total_scraped_jobs, total_scraped_time, total_jobs_saved

    # Parameters for job scraping
    search_term = "software engineer"
    location = "New York, NY"
    results_wanted = 20
    hours_old = 24
    country_code = "usa"

    # Start measuring total time
    start_time_total = time.time()

    # List of sites to scrape
    sites = ["google", "indeed", "glassdoor", "zip_recruiter"]

    # Method choice: You can choose between "critical", "atomic", or "reduction"
    method = "reduction"  # Change this to "atomic", "critical" for testing different methods
    cores = 8

    # Create a ThreadPoolExecutor to handle concurrent scraping
    with ThreadPoolExecutor(max_workers=cores) as executor:
        # Map benchmark_scraping function over the list of sites
        results = executor.map(
            benchmark_scraping,
            sites,
            [search_term] * len(sites),
            [location] * len(sites),
            [results_wanted] * len(sites),
            [hours_old] * len(sites),
            [country_code] * len(sites),
            [method] * len(sites)  # Passing the method choice to each thread
        )

        # Initialize job lists for each site (in case of reduction)
        all_jobs = []

        # Process the results as they come in (especially for reduction)
        for result in results:
            jobs, duration = result
            all_jobs.append(jobs)  # Collect all jobs in the reduction method

    # Total time for all scraping
    total_duration = time.time() - start_time_total
    print(f"Total scraping process took {total_duration:.2f} seconds")

    # Print the total number of jobs scraped and other race condition variables
    print(f"Total jobs scraped: {total_scraped_jobs}")
    print(f"Summition of the execution time of all processes: {total_scraped_time:.2f} seconds")
    print(f"Total jobs saved: {total_jobs_saved}")

    # Combine all jobs from different sites into one DataFrame (instead of list)
    combined_jobs_df = pd.concat(all_jobs, ignore_index=True)

    # Save results to CSV
    if not combined_jobs_df.empty:
        save_to_csv(combined_jobs_df, "combined_jobs.csv")


main()
