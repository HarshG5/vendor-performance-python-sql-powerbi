'''import necessary libraries'''
import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
from ingestion_db import ingest_db
import sqlite3

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

'''logging configuration'''
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
)

''' creating vendor sales summary table from the existing tables in the database'''
def create_vendor_summary(conn): 
        start_time = time.time()
        vendor_summary_table = pd.read_sql("""
                                   
                            with vendor_freight as (
                            select 
                            VendorNumber, 
                            avg(Freight/Quantity) as avg_freight_cost
                            from vendor_invoice group by VendorNumber),
                                   
                            purchase_summary as (select 
                                                a.VendorNumber,
                                                a.VendorName,
                                                a.Brand,
                                                b.Volume,
                                                avg(a.PurchasePrice) as purchase_price,
                                                sum(a.Quantity) as total_quantity,
                                                sum(a.Dollars) as total_dollars,
                                                avg(b.PurchasePrice) as Actual_Price
                                        from purchases as a
                                     join purchase_prices as b
                                     on a.Brand = b.Brand
                                     and a.VendorNumber = b.VendorNumber
                                     where a.purchaseprice > 0 and b.PurchasePrice > 0
                                     group by a.VendorNumber, a.VendorName, a.Brand),  

                            sales_summary as (SELECT VendorNo,
                            VendorName, 
                            Brand, 
                            Description,
                            sum(SalesDollars) as total_sales, 
                            sum(SalesQuantity) as total_sales_quantity, 
                            avg(SalesPrice) as sales_price, 
                            sum(ExciseTax) as total_excise_tax   
                            FROM sales
                            group by 1,2,3,4)

                                                                          
                            SELECT 
                                p.VendorNumber,
                                p.VendorName,
                                s.description,
                                p.Brand,
                                p.volume,
                                p.purchase_price,
                                p.total_quantity,
                                p.total_dollars,
                                p.Actual_Price,
                                s.total_sales,
                                s.total_sales_quantity,
                                s.sales_price,
                                s.total_excise_tax,
                                vi.avg_freight_cost
                            FROM purchase_summary as p
                            JOIN purchase_prices as pp
                                ON p.Brand = pp.Brand
                                AND p.VendorNumber = pp.VendorNumber
                            JOIN sales_summary as s
                                ON p.VendorNumber = s.VendorNo
                                AND p.Brand = s.Brand
                            join vendor_freight as vi
                                ON p.VendorNumber = vi.VendorNumber
                            GROUP BY 
                                p.VendorNumber,
                                p.VendorName,
                                p.Brand,
                                p.volume;
                        """, conn)
        end_time = time.time()
        logging.info(f"Vendor_sales_summary executed in {end_time - start_time} seconds")
        return vendor_summary_table
        

def clean_vendor_summary(vendor_summary_table):
        start_time = time.time()
        vendor_summary_table['VendorName'] = vendor_summary_table['VendorName'].str.strip()
        vendor_summary_table['Volume'] = vendor_summary_table['Volume'].astype('Float64')
        vendor_summary_table.info()
        end_time = time.time()
        logging.info(f"Data cleaning executed in {end_time - start_time} seconds")
        return vendor_summary_table

def vendor_sales_summary_add_metrics(vendor_summary_table):
        start_time = time.time()
        vendor_summary_table['gross_profit_per_unit'] = vendor_summary_table['sales_price'] - vendor_summary_table['purchase_price'] -vendor_summary_table['avg_freight_cost']- (vendor_summary_table['total_excise_tax']/vendor_summary_table['total_quantity'])
        vendor_summary_table['profit_margin'] = vendor_summary_table['gross_profit_per_unit'] / vendor_summary_table['sales_price']
        vendor_summary_table['stock_turnover_ratio'] = vendor_summary_table['total_sales_quantity'] / vendor_summary_table['total_quantity']
        end_time = time.time()
        logging.info(f"Metrics addition executed in {end_time - start_time} seconds")
        return vendor_summary_table



if __name__ == "__main__":
        conn = sqlite3.connect('vendor_performance.db')
        logging.info("Database connection established.")
        logging.info("Creating vendor sales summary table.")
        vendor_summary_table = create_vendor_summary(conn)
        logging.info(vendor_summary_table.head())
        logging.info("Cleaning vendor sales summary table.")
        vendor_cleaning_table = clean_vendor_summary(vendor_summary_table)
        logging.info(vendor_cleaning_table.head())
        logging.info("Adding metrics to vendor sales summary table.")
        vendor_final_table = vendor_sales_summary_add_metrics(vendor_cleaning_table)
        logging.info(vendor_final_table.head())
        logging.info("Ingesting vendor sales summary table into database.")
        ingest_db(vendor_final_table, 'vendor_sales_summary', conn)
        logging.info("Vendor sales summary table ingested successfully.")

