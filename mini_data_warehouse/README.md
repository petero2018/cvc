# Task 1

Discussion Points
• From a technical standpoint, what are your observations about the data extract?
• What potential challenges might arise when working with this data?
• Can you identify any data quality issues or inconsistencies?
• If you were responsible for designing this data extract, what additional
information would you include to improve usability?
• What would the end-to-end pipeline look like for ingesting this data into a
Snowflake data warehouse?



## 1.1 Technical observations
The extract combines event-level transactions (commitments, calls, distributions, valuations) with static attributes (fund size, sector, region). Dates are stored as strings in DD/MM/YYYY format, transaction_type is free-text, and transaction_index is inconsistently numeric. There are no clear surrogate keys, so uniqueness has to be inferred from natural keys (e.g. fund name + date). Static attributes such as fund size drift over time without effective dating.

## 1.2 Potential challenges
Because the extract is event-oriented, it is not an ideal single source for building dimensions; reference information for funds, companies, and geographies should come from a more authoritative source. Joins rely on free-text identifiers (fund name, company ID), which are error-prone. Inconsistent formats and missing attributes complicate transformation, and outliers such as ownership percentages greater than 100% can occur if fund sizes are not kept in sync with commitments.

## 1.3 Data quality issues

Country values appear in multiple formats (e.g. USA vs United States).

transaction_index appears as both integers and floating-point numbers.

region, country, and sector are missing for some companies.

Dates must be parsed from strings and may fail if formats vary.

Fund size values vary across rows without clear effective dating.

## 1.4 E2e pipeline

transaction related event data could be streamed into snowflake and based upon the latency requirements it's either dbt transformed to look up the reference data or spark transformed on the fly to supply near real time enriched dataset.
This however, only works if ids can be supplied with the event records and those ids can be looked up from another system that is integrated into snowflake.



# Task 2

## 2.1 

Additional Considerations:
• Some dates in the extract contain multiple valuations, but their significance
is unclear. The user has confirmed that their Excel output is correct but is
unsure how these multiple valuations should be handled.
• Clearly document any assumptions you make about data treatment in your
SQL solution.


* This is essentially the same logic as our fact_fund_nav_over_time model, but restricted to event dates only. *

## Assumptions about data treatment:

    - Multiple valuations on the same fund/date

        If more than one valuation exists for the same fund on the same date, I keep the highest valuation.
        Rationale: this avoids understating NAV. Alternatively, if a business rule exists for transaction_index ordering, that should be applied instead.

    - Flows relative to valuation dates
        CALLS are treated as positive inflows, DISTRIBUTIONS as negative.
        Flows are included if they occur after the valuation date and up to (and including) the report date.
        Flows on the same day as a valuation are excluded from that valuation date and will impact NAV from the following day onwards.

    - Report dates
        The NAV series is reported only on event dates (valuation or flow dates), consistent with the Excel example provided.
        If a full daily NAV is required, this logic could be applied to a date spine (e.g., dim_date).

    - Data parsing and cleansing

        Dates are assumed to be in DD/MM/YYYY format and parsed accordingly.
        Transaction amounts are cast to numeric; invalid values are treated as null.
        Fund names are trimmed and uppercased to ensure consistent matching.

## 2.2

The user now wants to see the same NAV calculation at the company level.
• The company_data table represents the total valuation of companies held
by the funds.
• Since CVC only owns a fraction of each fund, the company valuations
should be scaled down based on CVC's ownership of the fund.
• It is worth noting that the “fund_size” represents the latest size of the
fund, and not the fund size at the time of the transaction.


`fund_ownership_v → computes ownership% per fund as of each date = (cumulative COMMITMENTs up to that date) ÷ latest fund_size.`

`fact_company_nav_over_time_m_one → joins company valuations to fund_ownership_v on the same date and calculates Company NAV = Ownership × Company Valuation.`


