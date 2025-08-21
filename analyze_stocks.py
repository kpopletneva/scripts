import sys
import pandas as pd

"""
Merges account activity and portfolio positions csv reposts from Fidelity.
Drops some of the report columns.
Generates two tables:
    1. Table with dividend producing stocks, sorted by dividends earned.
    2. All other stocks. 

Use: 
python analyze_stocks.py <history_for_account>.csv <portfolio_positions>.csv
"""

columns = {
    "date": "Run Date", 
    "action": "Action", 
    "stock": "Symbol", 
    "dollars": "Amount ($)",
    "quantity": "Quantity",
    "price": "Last Price",
    "value": "Current Value",
    "cost_basis": "Average Cost Basis",
    "dollar_gain_loss": "Total Gain/Loss Dollar"
}

action = {
    "dividends": "DIVIDEND RECEIVED", 
    "fees": "FEE CHARGED", 
    "tax": "TAX PAID"
}

# Display 65 rows
pd.set_option('display.max_rows', 65)

if len(sys.argv) > 2:
        TRANSACTIONS = sys.argv[1]
        PORTFOLIO = sys.argv[2]

class GenerateReport():
    def parse_csv(self):
        transactions_df = pd.read_csv(TRANSACTIONS)
        portfolio_df = pd.read_csv(PORTFOLIO)
        # Convert history to datetime objects or replace with NaN
        transactions_df[columns["date"]] = pd.to_datetime(transactions_df[columns["date"]], errors="coerce")
        # Filter out rows with invalid or missing dates
        transactions_df = transactions_df.dropna(subset=[columns["date"]])
        transactions_df = transactions_df[
            (transactions_df[columns["action"]].str.contains(action["dividends"], case=False, na=False)) 
            | (transactions_df[columns["action"]].str.contains(action["fees"], case=False, na=False)) 
            | (transactions_df[columns["action"]].str.contains(action["tax"], case=False, na=False))
        ]
        transactions_df["Month_Name"] = transactions_df[columns["date"]].dt.month_name()

        months_used = transactions_df["Month_Name"].unique()
        dividend_stocks = transactions_df[columns["stock"]].unique()

        portfolio_df = portfolio_df[portfolio_df[columns["stock"]].str.len() < 10]
        portfolio_df = portfolio_df.filter(
            items=[
                columns["stock"], 
                columns["quantity"], 
                columns["price"], 
                columns["value"], 
                columns["cost_basis"], 
                columns["dollar_gain_loss"]
            ]
        )
        
        portfolio_df[columns["dollar_gain_loss"]] = portfolio_df[columns["dollar_gain_loss"]].replace(r"\$", "", regex=True)
        portfolio_df[columns["dollar_gain_loss"]] = portfolio_df[columns["dollar_gain_loss"]].replace(r"\+", "", regex=True)
        portfolio_df[columns["dollar_gain_loss"]] = pd.to_numeric(portfolio_df[columns["dollar_gain_loss"]], errors='coerce')

        if "SPAXX" in dividend_stocks:
            portfolio_df[columns["stock"]] = portfolio_df[columns["stock"]].replace("SPAXX**", "SPAXX")

        return transactions_df, portfolio_df, months_used, dividend_stocks

    def overall_stat(self, transactions_df, portfolio_df, dividend_stocks):
        stock_frequency = transactions_df[transactions_df[columns["action"]].str.contains(action["dividends"], case=False, na=False)]
        stock_frequency = stock_frequency[columns["stock"]].value_counts().reset_index()
        stock_dividends = transactions_df.groupby(columns["stock"])[columns["dollars"]].sum()
        dividend_stat = pd.merge(stock_frequency, stock_dividends, on=columns["stock"], how="outer")
        sorted_dividend_stat = dividend_stat.sort_values(by=[columns["dollars"]], ascending=False, ignore_index=True)
        sorted_dividend_stat = pd.merge(sorted_dividend_stat, portfolio_df, on=columns["stock"], how="left")
        other_stocks_stats = portfolio_df[~portfolio_df[columns["stock"]].isin(dividend_stocks)]
        sorted_other_stocks_stats = other_stocks_stats.sort_values(by=[columns["dollar_gain_loss"]], ascending=False, ignore_index=True)

        return sorted_dividend_stat, sorted_other_stocks_stats

    def monthly_stat(self, df, month):       
        monthly_df = df[df["Month_Name"] == month]
        monthly_stocks = monthly_df[columns["stock"]].unique()
        monthly_net_dividends = sum(self.net_dividends(monthly_df)).round(2)

        return monthly_df, monthly_net_dividends

    def net_dividends(self, df):
        # Dividends − Fees/Taxes
        net_dividends_spaxx = df[df[columns["stock"]] == "SPAXX"][columns["dollars"]].sum().round(2)
        net_dividends_other = df[df[columns["stock"]] != "SPAXX"][columns["dollars"]].sum().round(2)

        return net_dividends_spaxx, net_dividends_other

    def summary(self):
        transactions_df, portfolio_df, months_used, dividend_stocks = self.parse_csv()
       
        dividend_stocks_stats, other_stocks_stats = self.overall_stat(transactions_df, portfolio_df, dividend_stocks) 

        net_dividends_spaxx, net_dividends_other = self.net_dividends(dividend_stocks_stats)

        net_gain_loss_divident_stocks = dividend_stocks_stats[columns["dollar_gain_loss"]].sum().round(2)
        net_gain_loss_other_stocks = other_stocks_stats[columns["dollar_gain_loss"]].sum().round(2)

        dividend_stocks_stats.rename(columns={columns["dollars"]: "Dividends ($)"}, inplace=True)
        dividend_stocks_stats.rename(columns={"count": "Dividends Received (count)"}, inplace=True)
        dividend_stocks_stats.rename(columns={columns["dollar_gain_loss"]: "Total Gain/Loss ($)"}, inplace=True)
        other_stocks_stats.rename(columns={columns["dollar_gain_loss"]: "Total Gain/Loss ($)"}, inplace=True)

        print(f"\nStock that produce dividends, in {len(months_used)} mo.: \n\n {dividend_stocks_stats} \n")
        print(f"\nTotal dividends per SPAXX: ${net_dividends_spaxx}")
        print(f"Total dividends per other stocks: ${net_dividends_other} \n")
        print(f"\nStocks that doesn't produce dividends: \n\n {other_stocks_stats}") 

        print(f"\n\nTotal (gain - loss) if sell all dividend producing stocks will be: ${net_gain_loss_divident_stocks}")
        print(f"Total (gain - loss) if sell all other stocks will be: ${net_gain_loss_other_stocks}")

        print(f"\nStats calculated per {len(months_used)} momths: {months_used}")

if __name__ == "__main__":
    report = GenerateReport()
    report.summary()