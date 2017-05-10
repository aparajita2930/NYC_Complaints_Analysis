source /home/$USER/.bashrc

rm use_cases_data/overall_complaint_type_dist.out
rm use_cases_data/year_dist.out
rm use_cases_data/year_complaint_type_dist.out
rm use_cases_data/hour_dist.out
rm use_cases_data/hour_complaint_type_dist.out
rm use_cases_data/day_dist.out
rm use_cases_data/day_complaint_type_dist.out
rm use_cases_data/status_dist.out
rm use_cases_data/overall_closing_time_dist.out
rm use_cases_data/agency_closing_time_dist.out
rm use_cases_data/complaint_closing_time_dist.out
rm use_cases_data/city_closing_time_dist.out
rm use_cases_data/borough_closing_time_dist.out
rm use_cases_data/bour_dist.out
rm use_cases_data/types_by_city.out
rm use_cases_data/types_by_location.out
rm use_cases_data/location_dist.out
rm use_cases_data/day_time_dist.out

hfs -rm -r overall_complaint_type_dist.out
hfs -rm -r year_dist.out
hfs -rm -r year_complaint_type_dist.out
hfs -rm -r hour_dist.out
hfs -rm -r hour_complaint_type_dist.out
hfs -rm -r day_dist.out
hfs -rm -r day_complaint_type_dist.out
hfs -rm -r status_dist.out
hfs -rm -r overall_closing_time_dist.out
hfs -rm -r agency_closing_time_dist.out
hfs -rm -r complaint_closing_time_dist.out
hfs -rm -r city_closing_time_dist.out
hfs -rm -r borough_closing_time_dist.out
hfs -rm -r bour_dist.out
hfs -rm -r types_by_city.out
hfs -rm -r types_by_location.out
hfs -rm -r location_dist.out
hfs -rm -r heating_complaints_temp.out
hfs -rm -r heating_complaints_temp_summary.out
hfs -rm -r noise_complaint_dist.out
hfs -rm -r noise_complaint_dist_summary.out
hfs -rm -r homeless_*
hfs -rm -r combined_*
hfs -rm -r day_time_dist.out

spark-submit use_cases/complaint_type_distribution.py /user/ac5901/NYC_Complaints.csv/part-00000
spark-submit use_cases/closing_time_distribution.py /user/ac5901/NYC_Complaints.csv/part-00000
spark-submit use_cases/status_distribution.py /user/ac5901/NYC_Complaints.csv/part-00000
spark-submit use_cases/comp_count.py /user/ac5901/NYC_Complaints.csv/part-00000
spark-submit use_cases/day_time_distribution.py /user/ac5901/NYC_Complaints.csv/part-00000
spark-submit use_cases/heating_complaint_distribution.py /user/ac5901/NYC_Complaints.csv/part-00000
spark-submit use_cases/noise_collisions_distribution.py /user/ac5901/NYC_Complaints.csv/part-00000
spark-submit use_cases/homeless_income_distribution.py /user/ac5901/NYC_Complaints.csv/part-00000
echo "-------------------------------------------------"
# or do whatever with individual element of the array
hfs -getmerge overall_complaint_type_dist.out use_cases_data/overall_complaint_type_dist.out
hfs -getmerge year_dist.out use_cases_data/year_dist.out
hfs -getmerge year_complaint_type_dist.out use_cases_data/year_complaint_type_dist.out
hfs -getmerge hour_dist.out use_cases_data/hour_dist.out
hfs -getmerge hour_complaint_type_dist.out use_cases_data/hour_complaint_type_dist.out
hfs -getmerge day_dist.out use_cases_data/day_dist.out
hfs -getmerge day_complaint_type_dist.out use_cases_data/day_complaint_type_dist.out
hfs -getmerge status_dist.out use_cases_data/status_dist.out
hfs -getmerge overall_closing_time_dist.out use_cases_data/overall_closing_time_dist.out
hfs -getmerge agency_closing_time_dist.out use_cases_data/agency_closing_time_dist.out
hfs -getmerge complaint_closing_time_dist.out use_cases_data/complaint_closing_time_dist.out
hfs -getmerge city_closing_time_dist.out use_cases_data/city_closing_time_dist.out
hfs -getmerge borough_closing_time_dist.out use_cases_data/borough_closing_time_dist.out
hfs -getmerge bour_dist.out use_cases_data/bour_dist.out
hfs -getmerge types_by_city.out use_cases_data/types_by_city.out
hfs -getmerge types_by_location.out use_cases_data/types_by_location.out
hfs -getmerge location_dist.out use_cases_data/location_dist.out
hfs -getmerge day_time_dist.out use_cases_data/day_time_dist.out
hfs -getmerge heating_complaints_temp.out use_cases_data/heating_complaints_temp.out
hfs -getmerge heating_complaints_temp_summary.out use_cases_data/heating_complaints_temp_summary.out
hfs -getmerge noise_complaint_dist.out use_cases_data/noise_complaint_dist.out
hfs -getmerge noise_complaint_dist_summary.out use_cases_data/noise_complaint_dist_summary.out
hfs -getmerge homeless_complaints_combined_with_income_gap.out use_cases_data/homeless_complaints_combined_with_income_gap.out
hfs -getmerge combined_homeless_complaint_dist_summary.out use_cases_data/combined_homeless_complaint_dist_summary.out
hfs -getmerge homeless_assistance_complaints_with_income_gap.out use_cases_data/homeless_assistance_complaints_with_income_gap.out
hfs -getmerge homeless_assistance_complaint_dist_summary.out use_cases_data/homeless_assistance_complaint_dist_summary.out
hfs -getmerge homeless_encampment_complaints_with_income_gap.out use_cases_data/homeless_encampment_complaints_with_income_gap.out
hfs -getmerge homeless_encampment_complaint_dist_summary.out use_cases_data/homeless_encampment_complaint_dist_summary.out

echo "-------------------------------------------------"
       


