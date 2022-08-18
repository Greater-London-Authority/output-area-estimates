# Output Area Population Estimates and Components
Ben Corr & Wil Tonkiss  
August 2022

ONS Small Area Population Estimates (SAPE) area available for all output areas
in England & Wales. Full detailed component data are not currently published
for this geography.

All data used in this project are publicly available from the ONS website.


### Available data

#### Births
OA births data are published for the period 2002-2020 for all OAs in E&W.
This is the only complete component available.

#### Deaths
LSOA deaths by age group are published for all LSOAs in E&W.
OA total deaths are published for all OAs in E&W.

#### Migration
No migration data is currently published for geographies below LA.


### Deaths modelling
The OA deaths are modeled in a 2 stage process:   
1. LSOA deaths by sex and SYA are modeled using the deaths by grouped age at
LSOA and deaths by single year at OA. The LSOA SYA deaths are calculated using
an iterative proportional fitting method.  
2. OA deaths by sex and SYA are modeled using the deaths by SYA at
LSOA calculated in the previous step and total deaths at OA. As with the
LSOA deaths, the OA deaths are calculated using an iterative proportional
fitting method.


### Net migration calcualtion
Net migration is calculated as population change from the end of one year to the
end of the next minus births plus deaths.

Gross migration flows are not calculated as part of this process.

### Scripts
**oa_population_sya** - reads in raw xlsx files and compiles them into rds outputs.  
**oa_births_sex** -  reads in raw xlsx files and compiles them into rds outputs.  
**oa_deaths_totals** - reads in raw xlsx files and compiles them into rds outputs.  
**lsoa_deaths_sya** - Uses grouped-age LSOA data and SYA deaths data at LAD to calculated
LSOA SYA deaths using ipf.  
Warning: Takes 30 minutes.  
**oa_deaths_sya** - Uses LSOA SYA data calculated above with OA total deaths data to
calculate OA SYA data using ipf.  
Warning: Takes ~22 hours to complete.  
**oa_net_migration** - Uses simple differencing from the other components and
population change to calculated net migration.  
Warning: Takes 30 minutes.  

  
  
### Notes

**Geography**
This project currently standardises all Local Authorities to 2021 codes and boundaries (LAD21CD).

**Ageing-on**
When ageing a population forward a year births are normally added after the
existing population population has been aged on.
An alternate method, used here, is to start with births and
subtract 1 year and make the age -1. This is then bound to the population and
everything is moved forward 1 year and 1 age increment.  
  
  
