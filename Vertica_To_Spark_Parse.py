
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row


#df.printSchema().show()
#logFile = "file:/Users/zzhao/Downloads/spark-1.6.0-bin-hadoop2.4/README.md"  # Should be some file on your system
#sc = SparkContext("local", "Simple App")
#logData = sc.textFile(logFile).cache()#

#numAs = logData.filter(lambda s: 'a' in s).count()
#numBs = logData.filter(lambda s: 'b' in s).count()

#print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

import os
os.environ['SPARK_CLASSPATH'] = "~/Desktop/Spark_Example/vertica-jdbc-7.0.2-1.jar"

fileName = "/Users/zzhao/Desktop/Spark_Example/parseFile.txt"
SEARCH_PATH = "serp_algo_staging"; 

query = """(SELECT s.id, s.search_term_type,s.Term,s.Search_Date,s.Rank,s.subRank,
			CASE WHEN s.has_map='true' then 1 else 0 end as has_map,
			s.page,s.has_indents,s.indent_count,s.url,
            CASE WHEN trim(s.url) like '%://%' THEN substring(trim(s.url),instr(trim(s.url),'://')+3,999) else trim(s.url) end as url_scrubhttp,
			case when trim(s.url) like '%/' then substring(trim(s.url),1,length(trim(s.url))-1) else trim(s.url) end as url_scrublastslash,
			case when trim(s.url) like '%/' then substring(trim(s.url),1,length(trim(s.url))-1) else trim(s.url) end as url_scrub_w_alias,
			trim(s.url) as subdomain,
			trim(s.url) as domain,
			ifnull(s.Rating,s.review_link_text) as rating_raw,
			--review_link_text,
			Rating as rating,
			review_link_text as reviews,
			google_places_link,
			0 as is_google,
			0 as is_review_site,
			Tenant_Id || '_' || Location_Code as location_id,
			cast('' as varchar(255)) as hp,
			0 as is_hp,
			cast('' as varchar(255)) as hp_subdomain,
			cast('' as varchar(255)) as hp_domain,
			0 as is_hp_domain,
			'' as src,
			0 as is_my_src,
			'' as is_comp,
			0 as is_src2,
			cast(-1.0 as float) as src_base_rating,
			cast(-1.0 as float) as src_avg_rank,
			-1 as src_base_reviews,
			'' as industry,
			Tenant_Id,
			Location_Code,
			'US' as country,
			'' as top_src,
			'' as dom_src,
			cast(0.0 as float) as dom_wgt,
			cast(0.0 as float) as goog_wgt,
			0 as sum_srcs,
			cast(0.0 as float) as src_rating,
			0 as src_revs,
			cast(0.0 as float) as src_match_score,
			0 as is_bad_domain,
			0 as hp_count
			from serp_algo.AutoSerpData as s LIMIT 10) as i"""
queryTest = """(select l.id,l.tenant_id,country,
case when country <> 'US' and country <>'GB' then 'US' else country end as country_lookup,
ifnull(l.industry,t.industry) as industry,
l.web,
c.hp_count
 from  r4e_mongo.mongo_repbiz_location l
 join  r4e_mongo.mongo_repbiz_tenant_configurations t on t.id=l.tenant_id
 left join serp_algo_staging.web_page_cts c on l.web=c.web) as s"""

sc = SparkContext("local[4]", 'Vertica_To_Spark_Parse')
sqlctx = SQLContext(sc)


sql_file = open(fileName)
indi_sql = "";
sql_list = [];
df_list =[];


for line in sql_file:
	if ";" in line:
		tmp = line.split(";");
		indi_sql+=str(tmp[0]);
		sql_list.append(indi_sql);
		
		if len(tmp) == 2 :
			indi_sql = str(tmp[1]);
		else :
			indi_sql = "";
	else :
		indi_sql += line;

for line in sql_list:
	line = line.replace("\n", " ");
	sql_string_list = line.split(" ");
	i = 0;
	table_name = "";
	executable_sql = "(";
	#print(sql_string_list);
	while i < len(sql_string_list):
		if "--" in sql_string_list[i].lower():
			# do nothing
			i+=1;
			continue;
		elif sql_string_list[i].lower() == 'drop':
			#empty table on drop
			break;
		elif sql_string_list[i].lower() == 'update':
			#insert into table
			break;
		elif sql_string_list[i].lower() == 'create':
			table_name = sql_string_list[i+2];
			i +=3;
		elif sql_string_list[i].lower() == 'select':

			executable_sql += "select ";
		#elif sql_string_list[i].lower() == 'from':
			#add search path if does not exist


		else :
			executable_sql += sql_string_list[i].lower() + " ";
			
		i += 1;

	
	print("executable_sql: " + executable_sql);
	# create DF using table name and execute sql
	if executable_sql != "(" and table_name != "":

		executable_sql +=" LIMIT 10) as " + table_name;
		df_list.append(sqlctx.read.format('jdbc').options(url="jdbc:vertica://10.101.122.149:5433/rwarehouse?user=rep_engine_user&password=r3p_3ng1n3&searchpath="+SEARCH_PATH, dbtable=executable_sql));


	table_name = "";
	executable_sql = "";

	
for x in df_list:
	x.load().show();
#	df.show();

#df_list[0].load();

