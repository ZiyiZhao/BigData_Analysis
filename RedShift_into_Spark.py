
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import psycopg2

#droping temp table
def drop(table_name):
	if sqlctx.tableNames().count(table_name) == 1:
		sqlctx.dropTempTable(table_name);	
	return;

#creating new table
def create(table_name, expression):
	df = sqlctx.sql(expression);
	#if the tables are not temp tables, truncate in Redshift
	if(table_name != 'sub_table' and table_name != 'joined_table' and table_name!='autoserpdata_urls_from_serp_1'):
		cur.execute("TRUNCATE TABLE " + outputSchema + "." + table_name);
	#these tables are used later for update, will write to parquet/redshift after the last update
	if(table_name == 'autoserpdata_addl' or table_name =='location_url_addl' or table_name =='autoserpdata_score_inputs3' or table_name =='autoserpdata_score_agg' or table_name == 'autoserpdata_score_agg1' or table_name == 'sub_table' or table_name == 'joined_table'):
		df.registerTempTable(table_name);
	else: 
		#Redshift does not support join but not selecting anything from the joined table, so drop the additional column
		if table_name == 'autoserpdata_score_agg_max':
			df = df.drop("max_abs_score");
		elif table_name == 'serp_summary_data_src3':
			df = df.drop("is_my_src_n");
		df.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/" + table_name + ".parquet");
		df.write.format("com.databricks.spark.redshift").options(tempdir = tempdir, url = redshift_url, dbtable = "%s.%s" % (outputSchema, table_name)).mode("append").save();
	return;

#updating existing table
def update(table_name, alias, column_name, expression):
	df = sqlctx.sql("select * from " + table_name + " as " + alias);
	df = df.withColumn(column_name, expr(expression));
	df.registerTempTable(table_name);
	print("in update");
	return;

#final 
redshift_url = "jdbc:redshift://test.cv741ke2k9an.us-east-1.redshift.amazonaws.com:5439/test?user=test&password=Test1234";
tempdir = "s3n://serp-spark/temp/";
outputSchema = "serp_algo_test";

#creating spark context
sc = SparkContext("local[8]", 'serp-spark');
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAJPJ5OBYD74NHT3TA");
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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");

#need hive context for Windowing functions
sqlctx = SQLContext(sc);
hc = HiveContext(sc);
conn = psycopg2.connect(database = "test", user = "test", password = "Test1234", host = "test.cv741ke2k9an.us-east-1.redshift.amazonaws.com", port = "5439");
cur = conn.cursor();


#loading data into Spark from RedShift and write to parquet file
#spark operates on the object's hierarchy tree everytime a object is called, so operations build up over time, and more memory/time is consumed
autoSerpData = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable= "serp_algo_test.AutoSerpData").load();
autoSerpData.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/autoserpdata.parquet");

mongo_repbiz_location = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable= "r4e_mongo.mongo_repbiz_location").load();
mongo_repbiz_location.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet");

mongo_repbiz_tenant_configurations = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable = "r4e_mongo.mongo_repbiz_tenant_configurations").load();
mongo_repbiz_tenant_configurations.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/mongo_repbiz_tenant_configurations.parquet");

bad_domains = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable = "bnull_db.bad_domains").load();
bad_domains.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/bad_domains.parquet");

mongo_repbiz_location_url = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable = "r4e_mongo.mongo_repbiz_location_url").load();
mongo_repbiz_location_url.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/mongo_repbiz_location_url1.parquet");

mongo_repbiz_sources_weights = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable = "r4e_mongo.mongo_repbiz_sources_weights").load();
mongo_repbiz_sources_weights.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/mongo_repbiz_sources_weights.parquet");

mongo_repbiz_rating_summaries = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable = "r4e_mongo.mongo_repbiz_rating_summaries").load();
mongo_repbiz_rating_summaries.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/mongo_repbiz_rating_summaries.parquet");

mongo_repbiz_industries = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable = "r4e_mongo.mongo_repbiz_industries").load();
mongo_repbiz_industries.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/mongo_repbiz_industries.parquet");

mongo_repbiz_industries_thresholds = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable = "r4e_mongo.mongo_repbiz_industries_thresholds").load();
mongo_repbiz_industries_thresholds.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/mongo_repbiz_industries_thresholds.parquet");

position_parameter = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable = "bnull_db.position_parameter").load();
position_parameter.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/position_parameter.parquet");

mongo_repbiz_scores = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable = "r4e_mongo.mongo_repbiz_scores").load();
mongo_repbiz_scores.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/mongo_repbiz_scores.parquet");

repbiz_sources_weights_calculated = sqlctx.read.format("com.databricks.spark.redshift").options(url=redshift_url, tempdir = tempdir, dbtable = "serp_algo_staging.repbiz_sources_weights_calculated").load();
repbiz_sources_weights_calculated.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/repbiz_sources_weights_calculated.parquet");


create("autoserpdata_addl", "select s.id, s.search_term_type,s.term,s.search_date,s.rank,s.subrank, case when s.has_map=true then 1 else 0 end as has_map, " \
							+ "s.page,s.has_indents,s.indent_count, " \
							+ "s.url, "\
							+ "case when trim(s.url) like '%://%' then substring(trim(s.url),instr(trim(s.url), '://')+3,999) else trim(s.url) end as url_scrubhttp, " \
							+ "case when trim(s.url) like '%/' then substring(trim(s.url),1,length(trim(s.url))-1) else trim(s.url) end as url_scrublastslash, "\
							+ "case when trim(s.url) like '%/' then substring(trim(s.url),1,length(trim(s.url))-1) else trim(s.url) end as url_scrub_w_alias, "\
							+ "trim(s.url) as subdomain, "\
							+ "trim(s.url) as domain, "\
							+ "if(s.rating is not null, s.rating,s.review_link_text) as rating_raw, "\
							+ "rating as rating, "\
							+ "review_link_text as reviews, "\
							+ "google_places_link, "\
							+ "0 as is_google, "\
							+ "0 as is_review_site, "\
							+ "CONCAT(tenant_id, '_', location_code) as location_id, "\
							+ "cast('' as varchar(255)) as hp, "\
							+ "0 as is_hp, "\
							+ "cast('' as varchar(255)) as hp_subdomain, "\
							+ "cast('' as varchar(255)) as hp_domain, "\
							+ "0 as is_hp_domain, "
							+ "'' as src, "\
							+ "0 as is_my_src, "\
							+ "'' as is_comp, "\
							+ "0 as is_src2, "\
							+ "cast(-1.0 as float) as src_base_rating, "\
							+ "cast(-1.0 as float) as src_avg_rank, "\
							+ "-1 as src_base_reviews, "\
							+ "'' as industry, "\
							+ "tenant_id, "\
							+ "location_code, "\
							+ "'US' as country, "\
							+ "'' as top_src, "\
							+ "'' as dom_src, "\
							+ "cast(0.0 as float) as dom_wgt, "\
							+ "cast(0.0 as float) as goog_wgt, "\
							+ "0 as sum_srcs, "\
							+ "cast(0.0 as float) as src_rating, "\
							+ "0 as src_revs, "\
							+ "cast(0.0 as float) as src_match_score, "\
							+ "0 as is_bad_domain, "\
							+ "0 as hp_count "\
							+ "from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata.parquet` s");

create("web_page_cts", "select web, count(*) as hp_count "\
						+ "from parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` "\
						+ "group by web "\
						+ "order by hp_count desc");

create("location_addl", "select l.id,l.tenant_id,country, "\
						+ "case when country <> 'US' and country <>'GB' then 'US' else country end as country_lookup, "\
						+ "if(l.industry is null,t.industry, l.industry) as industry, "\
						+ "l.web, "\
						+ "c.hp_count "\
						+ "from parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` l "\
						+ "join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_tenant_configurations.parquet` t on t.id=l.tenant_id "\
						+ "left join parquet.`s3n://serp-spark/data-comp-pass/web_page_cts.parquet` c on l.web=c.web");


drop("sub_table");
create("sub_table", "select t.id as id_t, t.industry as industry_t from parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_tenant_configurations.parquet` t");

drop("joined_table");
create("joined_table", "select * from autoserpdata_addl s left join sub_table t on s.tenant_id=t.id_t");

drop("autoserpdata_addl");
create("autoserpdata_addl", "select id, search_term_type, term, search_date, rank, subrank, has_map, page, has_indents, "\
							+ "indent_count, url, url_scrubhttp, url_scrublastslash, url_scrub_w_alias, subdomain, "\
							+ "domain, rating_raw, rating, reviews, google_places_link, is_google, is_review_site, "\
							+ "location_id, hp, is_hp, hp_subdomain, hp_domain, is_hp_domain, src, is_my_src, is_comp, "\
							+ "is_src2, src_base_rating, src_avg_rank, src_base_reviews, industry_t as industry, tenant_id, "\
							+ "location_code, country, top_src, dom_src, dom_wgt, goog_wgt, sum_srcs, src_rating, src_revs, "\
							+ "src_match_score, is_bad_domain, hp_count from joined_table");

drop("sub_table");
create("sub_table", "select l.web as web_l, l.industry as industry_l, la.country_lookup as country_lookup_l, l.id as id_l, "\
					+ "la.hp_count as hp_count_l, l.tenant_id as tenant_id_l, l.code as code_l from parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` l "\
					+ "join parquet.`s3n://serp-spark/data-comp-pass/location_addl.parquet` la on l.id=la.id");

drop("joined_table");
create("joined_table", "select * from autoserpdata_addl s left join sub_table l on s.location_code=l.code_l and s.tenant_id=l.tenant_id_l");

drop("autoserpdata_addl");
create("autoserpdata_addl", "select id, search_term_type, term, search_date, rank, subrank, has_map, page, has_indents, indent_count, url, url_scrubhttp, url_scrublastslash, url_scrub_w_alias, subdomain, domain, rating_raw, rating, reviews, google_places_link, is_google, is_review_site, id_l as location_id, web_l as hp, is_hp, hp_subdomain, hp_domain, is_hp_domain, src, is_my_src, is_comp, is_src2, src_base_rating, src_avg_rank, src_base_reviews, if(industry_l is null, industry, industry_l) as industry, tenant_id, location_code, if(country_lookup_l is null, country, country_lookup_l) as country, top_src, dom_src, dom_wgt, goog_wgt, sum_srcs, src_rating, src_revs, src_match_score, is_bad_domain, if(hp_count_l is null, hp_count, hp_count_l) as hp_count from joined_table");



df_autoSerp = sqlctx.sql("select * from autoserpdata_addl as s");

df_autoSerp = df_autoSerp.withColumn("url_scrub_w_alias", expr("case when trim(url_scrub_w_alias) like '%://%' then substring(trim(url_scrub_w_alias),instr(trim(url_scrub_w_alias),'://')+3,999) else trim(url_scrub_w_alias) end" ));
df_autoSerp = df_autoSerp.withColumn("hp", expr("case when trim(hp) like '%://%' then substring(trim(hp),instr(trim(hp),'://')+3,999) else trim(hp) end" ));
df_autoSerp = df_autoSerp.withColumn("hp", expr("case when trim(hp) like '%/' then substring(trim(hp),1,length(trim(hp))-1) else trim(hp) end" ));
df_autoSerp = df_autoSerp.withColumn("url_scrub_w_alias", expr("if(url_scrub_w_alias = \"kaiserpermanente.org\", 'kp.org', trim(url_scrub_w_alias))" ));
df_autoSerp = df_autoSerp.withColumn("url_scrub_w_alias", expr("case when trim(url_scrub_w_alias) like 'www.%' then substring(trim(url_scrub_w_alias),5,999) else trim(url_scrub_w_alias) end" ));
df_autoSerp = df_autoSerp.withColumn("hp", expr("case when trim(hp) like 'www.%' then substring(trim(hp),5,999) else trim(hp) end" ));
df_autoSerp = df_autoSerp.withColumn("hp", expr("if(hp = \"kaiserpermanente.org\", 'kp.org', hp)" ));
df_autoSerp = df_autoSerp.withColumn("hp_subdomain", expr("case when trim(hp) like '%/%' then substring(trim(hp),1,instr(trim(hp),'/')-1) else trim(hp) end" ));
df_autoSerp = df_autoSerp.withColumn("hp_domain", expr("case when hp_subdomain like '%.%.%' then substring(hp_subdomain, instr(hp_subdomain,'.')+1) else hp_subdomain end" ));
df_autoSerp = df_autoSerp.withColumn("subdomain", expr("case when trim(url_scrubhttp) like '%/%' then substring(trim(url_scrubhttp),1,instr(trim(url_scrubhttp),'/')-1) else trim(url_scrubhttp) end" ));
df_autoSerp = df_autoSerp.withColumn("domain", expr("case when subdomain like '%.%.%' then substring(subdomain, instr(subdomain,'.')+1) else subdomain end" ));
df_autoSerp = df_autoSerp.withColumn("is_hp", expr("case when trim(hp)=url_scrub_w_alias and hp_count<=3 then 1 else 0 end" ));
df_autoSerp = df_autoSerp.withColumn("is_hp_domain", expr("case when trim(domain)=trim(hp_domain) then 1 else 0 end" ));
df_autoSerp = df_autoSerp.withColumn("is_google", expr("case when (has_map = 1 or google_places_link is not null) then 1 else 0 end" ));

drop("autoserpdata_addl");
df_autoSerp.registerTempTable("autoserpdata_addl");

#update with joins
temp_df = sqlctx.sql("select s.*, if(s.location_id=u.id and length(s.url)>3 and (instr(s.url,u.url)>0 or instr(u.url,s.url)>0), u.source, s.src) as source_u, if(s.location_id=u.id and length(s.url)>3 and (instr(s.url,u.url)>0 or instr(u.url,s.url)>0), 1, 0) as is_my_src_u, if(s.location_id=u.id and length(s.url)>3 and (instr(s.url,u.url)>0 or instr(u.url,s.url)>0), u.is_competitor, s.is_comp) as is_comp_u from autoserpdata_addl s left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location_url1.parquet` u on s.location_id=u.id and length(s.url)>3 and (instr(s.url,u.url)>0 or instr(u.url,s.url)>0)");

temp_df = temp_df.drop("src").withColumnRenamed("source_u","src").drop("is_my_src").withColumnRenamed("is_my_src_u", "is_my_src").drop("is_comp").withColumnRenamed("is_comp_u","is_comp");
drop("autoserpdata_addl");
temp_df.registerTempTable("autoserpdata_addl");


temp_df = sqlctx.sql("select s.*, if(s.location_id=u.id and s.google_places_link is not null and (instr(s.google_places_link,u.url)>0 or instr(u.url,s.google_places_link)>0), u.source, s.src) as source_u, if(s.location_id=u.id and s.google_places_link is not null and (instr(s.google_places_link,u.url)>0 or instr(u.url,s.google_places_link)>0), 1, 0) as is_my_src_u, if(s.location_id=u.id and s.google_places_link is not null and (instr(s.google_places_link,u.url)>0 or instr(u.url,s.google_places_link)>0), u.is_competitor, s.is_comp) as is_comp_u from autoserpdata_addl s left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location_url1.parquet` u on s.location_id=u.id and s.google_places_link is not null and (instr(s.google_places_link,u.url)>0 or instr(u.url,s.google_places_link)>0)");

temp_df = temp_df.drop("src").withColumnRenamed("source_u","src").drop("is_my_src").withColumnRenamed("is_my_src_u", "is_my_src").drop("is_comp").withColumnRenamed("is_comp_u","is_comp");

drop("autoserpdata_addl");
temp_df.registerTempTable("autoserpdata_addl");


drop("autoserpdata_baselines");
create("autoserpdata_baselines", "select industry,domain,count(*) as ct, "\
								+ "avg(a.rank) as avg_rank, "\
								+ "avg(cast(a.rating as float)) avg_rating, "\
								+ "avg(cast(reviews as int)) avg_reviews, "\
								+ "avg(is_google) as perc_google "\
								+ "from autoserpdata_addl a "\
								+ "where (a.rating <> '' and is_google=0) "\
								+ "and a.reviews <> '' "\
								+ "group by industry,domain "\
								+ "order by ct desc");


temp_df = sqlctx.sql("select s.*, if(s.domain=l.domain and s.industry=l.industry, 1, trim(is_src2)) as is_src2_n, if(s.domain=l.domain and s.industry=l.industry, cast(avg_rating as string),  trim(src_base_rating)) as src_base_rating_n, if(s.domain=l.domain and s.industry=l.industry, avg_reviews, trim(src_base_reviews)) as src_base_reviews_n, if(s.domain=l.domain and s.industry=l.industry, l.avg_rank, trim(src_avg_rank)) as src_avg_rank_n from autoserpdata_addl s left join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_baselines.parquet` l on s.domain=l.domain and s.industry=l.industry");

temp_df = temp_df.drop("is_src2").withColumnRenamed("is_src2_n","is_src2").drop("src_base_rating").withColumnRenamed("src_base_rating_n", "src_base_rating").drop("src_base_reviews").withColumnRenamed("src_base_reviews_n","src_base_reviews").drop("src_avg_rank").withColumnRenamed("src_avg_rank_n","src_avg_rank");

drop("autoserpdata_addl");
temp_df.registerTempTable("autoserpdata_addl");


create("location_url_addl", "select u.id,u.source,u.url,if(l.industry is null,tc.industry, l.industry) as industry,w.weight, "\
							+ "case when trim(u.url) like '%://%' then substring(trim(u.url),instr(trim(u.url),'://')+3,999) else trim(u.url) end as url_scrubhttp, "\
							+ "u.url as subdomain, "\
							+ "u.url as domain, "\
							+ "is_competitor, "\
							+ "la.country_lookup, "\
							+ "sum.num_ratings, "\
							+ "sum.num_display_ratings, "\
							+ "sum.overall_rating, "\
							+ "sum.created_date, "\
							+ "case when a.url is not null then 1 else 0 end as in_serp, "\
							+ "case when length(a.rating_raw)>2 then 1 else 0 end as has_review_info, "\
							+ "a.rank,a.rating,a.reviews "\
							+ "from parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location_url1.parquet` u "\
							+ "left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` l on l.id=u.id "\
							+ "left join parquet.`s3n://serp-spark/data-comp-pass/location_addl.parquet` la on l.id=la.id "\
							+ "left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_tenant_configurations.parquet` tc on l.tenant_id=tc.id "\
							+ "left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_sources_weights.parquet` w on if(l.industry is null,tc.industry, l.industry)=w.industry and u.source=w.id and la.country_lookup=w.country "\
							+ "left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_rating_summaries.parquet` sum on u.url=sum.url "\
							+ "left join autoserpdata_addl a on u.url=a.url and a.location_id=u.id");


df_loc = sqlctx.sql("select * from location_url_addl as s");

df_loc = df_loc.withColumn("subdomain" , expr("case when trim(url_scrubhttp) like '%/%' then substring(trim(url_scrubhttp),1,instr(trim(url_scrubhttp),'/')-1) else trim(url_scrubhttp) end" ));
df_loc = df_loc.withColumn("domain" , expr("case when subdomain like '%.%.%' then substring(subdomain, instr(subdomain,'.')+1) else subdomain end" ));

df_loc.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/location_url_addl.parquet");

cur.execute("TRUNCATE TABLE " + outputSchema + ".location_url_addl");
df_loc.write.format("com.databricks.spark.redshift").options(tempdir = tempdir, url = redshift_url, dbtable = "%s.%s" % (outputSchema, "location_url_addl")).mode("append").save();

create("src_domain", "select source,domain,industry,country_lookup,weight, "\
					+ "count(*) as ct "\
					+ "from parquet.`s3n://serp-spark/data-comp-pass/location_url_addl.parquet` "\
					+ "group by source,domain,industry,country_lookup,weight "\
					+ "order by ct desc");


temp_df = sqlctx.sql("select s.*, if(agg.domain=s.domain and agg.country_lookup=s.country and agg.industry=s.industry and cast(agg.weight as float)> cast(dom_wgt as float) and agg.weight is not null, agg.source, dom_src) as dom_src_n, if(agg.domain=s.domain and agg.country_lookup=s.country and agg.industry=s.industry and cast(agg.weight as float)> cast(dom_wgt as float) and agg.weight is not null, cast(agg.weight as float), dom_wgt) as dom_wgt_n, if(agg.domain=s.domain and agg.country_lookup=s.country and agg.industry=s.industry and cast(agg.weight as float)> cast(dom_wgt as float) and agg.weight is not null, agg.source, top_src) as top_src_n from autoserpdata_addl s left join parquet.`s3n://serp-spark/data-comp-pass/src_domain.parquet` agg on agg.domain=s.domain and agg.country_lookup=s.country and agg.industry=s.industry and cast(agg.weight as float)> cast(dom_wgt as float) and agg.weight is not null");


temp_df = temp_df.drop("dom_src").withColumnRenamed("dom_src_n","dom_src").drop("dom_wgt").withColumnRenamed("dom_wgt_n","dom_wgt").drop("top_src").withColumnRenamed("top_src_n","top_src");
drop("autoserpdata_addl");
temp_df.registerTempTable("autoserpdata_addl");



temp_df = sqlctx.sql("select s.*, if(agg.domain='google.com' and s.is_google=1 and agg.country_lookup=s.country and agg.industry=s.industry and cast(agg.weight as float)> cast(goog_wgt as float) and agg.weight is not null, cast(agg.weight as float), goog_wgt) as goog_wgt_n, if(agg.domain='google.com' and s.is_google=1 and agg.country_lookup=s.country and agg.industry=s.industry and cast(agg.weight as float)>cast(goog_wgt as float) and agg.weight is not null and cast(agg.weight as float)>cast(dom_wgt as float), 'GOOGLE_PLACES', top_src) as top_src_n from autoserpdata_addl s left join parquet.`s3n://serp-spark/data-comp-pass/src_domain.parquet` agg on agg.domain='google.com' and s.is_google=1 and agg.country_lookup=s.country and agg.industry=s.industry and agg.weight>goog_wgt and agg.weight is not null");

temp_df = temp_df.drop("goog_wgt").withColumnRenamed("goog_wgt_n","goog_wgt").drop("top_src").withColumnRenamed("top_src_n","top_src");
drop("autoserpdata_addl");
temp_df.registerTempTable("autoserpdata_addl");


create("location_url_addl2", "select id, source, industry,is_competitor,country_lookup, "\
							+ "count(*) as ct,avg(num_ratings) as num_ratings, avg(overall_rating) as overall_rating "\
							+ "from parquet.`s3n://serp-spark/data-comp-pass/location_url_addl.parquet` "\
							+ "group by id,source,industry,is_competitor,country_lookup");							


temp_df = sqlctx.sql("select s.*, if(lua.id=s.location_id and lua.source=s.dom_src and lua.is_competitor='F', lua.ct, sum_srcs) as sum_srcs_n, if(lua.id=s.location_id and lua.source=s.dom_src and lua.is_competitor='F', lua.overall_rating, src_rating) as src_rating_n, if(lua.id=s.location_id and lua.source=s.dom_src and lua.is_competitor='F', lua.num_ratings, src_revs) as src_revs_n from autoserpdata_addl s left join parquet.`s3n://serp-spark/data-comp-pass/location_url_addl2.parquet` lua on (lua.id=s.location_id and lua.source=s.dom_src and lua.is_competitor='F')");

temp_df = temp_df.drop("sum_srcs").withColumnRenamed("sum_srcs_n","sum_srcs").drop("src_rating").withColumnRenamed("src_rating_n","src_rating").drop("src_revs").withColumnRenamed("src_revs_n","src_revs");

drop("autoserpdata_addl");
temp_df.registerTempTable("autoserpdata_addl");


update("autoserpdata_addl", "s", "src_match_score", "case when sum_srcs>0 then (case when sum_srcs>1 then .5 else 1 - "\
 													+ "abs(if(cast(rating as float) is null,0, cast(rating as float))-if(src_rating is null,0, src_rating)/5.0) - "\
													+ "(case when cast(reviews as float)>floor(src_revs*.85) and cast(reviews as float)<ceiling(src_revs*1.15) then 0 else .3 end) end )"\
													+ "else trim(src_match_score) end");


#cur.execute("TRUNCATE TABLE " + outputSchema + ".autoserpdata_addl");
#sqlctx.sql("select * from autoserpdata_addl").write.format("com.databricks.spark.redshift").options(tempdir = tempdir, url = redshift_url, dbtable = "%s.%s" % (outputSchema, "autoserpdata_addl")).mode("append").save();


create("autoserpdata_google_stats", "select u.id as location_id,u.is_competitor,u.source, "\
									+ "u.url,sum.num_ratings,sum.num_display_ratings,sum.overall_rating,sum.created_date, "\
									+ "w.industry,w.weight "\
									+ " from parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location_url1.parquet` u "\
									+ " join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_rating_summaries.parquet` sum on u.url=sum.url "\
									+ " join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` l on u.id=l.id "\
									+ " join parquet.`s3n://serp-spark/data-comp-pass/location_addl.parquet` la on u.id=la.id "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_tenant_configurations.parquet` tc on l.tenant_id=tc.id "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_sources_weights.parquet` w on if(l.industry is null,tc.industry, l.industry)=w.industry and u.source=w.id and la.country_lookup=w.country "\
									+ "where u.source like 'GOOGLE%'");

create("autoserpdata_google_stats2", "select location_id,is_competitor,industry, "\
									+ "count(*) as ct, "\
									+ "avg(weight) as weight, "\
									+ "avg(num_ratings) as num_ratings, "\
									+ "avg(num_display_ratings) as num_display_ratings, "\
									+ "avg(overall_rating) as overall_rating, "\
									+ "max(created_date) as created_date "\
									+ "from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_google_stats.parquet` "\
									+ "group by location_id,is_competitor,industry");



create("autoserpdata_score_inputs", "select a.search_term_type,a.term,a.search_date,a.tenant_id, "\
									+ "a.location_id, a.rank, a.subrank,a.country, "\
									+ "a.has_map, "\
									+ "a.page,a.has_indents,a.indent_count, "\
									+ "a.rating,a.reviews,a.is_google,a.is_review_site,a.is_hp,a.is_hp_domain,a.hp_count, "\
									+ "a.src,a.is_my_src,a.is_comp,a.is_src2,a.src_base_rating,cast(a.src_base_reviews as integer),a.src_avg_rank, "\
									+ "a.industry, "\
									+ "it.average_star_rating,i.category,it.characteristic_frequency, "\
									+ "it.ideal_review_num,it.min_number_total_reviews, "\
									+ "if(g2.weight is null, cast(w.weight as float), cast(g2.weight as float)) as weight,dom_wgt,goog_wgt, "\
									+ "if(g2.num_ratings is null,sum.num_ratings, g2.num_ratings) as num_ratings, "\
									+ "if(g2.num_display_ratings is null,sum.num_display_ratings, g2.num_display_ratings) as num_display_ratings, "\
									+ "if(cast(sum.overall_rating as float) is null,g2.overall_rating, cast(sum.overall_rating as float)) as overall_rating, "\
									+ "if(sum.created_date is null,g2.created_date, sum.created_date) as created_date, "\
									+ "a.rating_raw, "\
									+ "a.domain,a.subdomain,a.hp_domain,a.url "\
									+ "from autoserpdata_addl a "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_industries.parquet` i on a.industry=i.id "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_industries_thresholds.parquet` it on i.id = it.id and it.country = 'DEFAULT' "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_rating_summaries.parquet` sum on a.url=sum.url "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_sources_weights.parquet` w on a.industry=w.industry and a.src=w.id and a.country=w.country "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_google_stats2.parquet` g2 on a.location_id=g2.location_id and a.is_google=1 and g2.is_competitor='F'");

create("autoserpdata_locations", "select location_id,count(*) from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_inputs.parquet` group by location_id");

create("src_domain_data", "select industry, source, domain, weight, count(*) "\
							+ "from parquet.`s3n://serp-spark/data-comp-pass/location_url_addl.parquet` "\
							+ "group by industry,source,domain,weight "\
							+ "order by weight desc");

drop("location_url_addl3");
create("location_url_addl3", "select industry,domain,country_lookup,avg(weight) as weight "\
							+ "from parquet.`s3n://serp-spark/data-comp-pass/location_url_addl.parquet` "\
							+ "group by industry,domain,country_lookup");

sqlctx.sql("select industry, domain, rating_raw, url, url_scrublastslash, tenant_id, location_code from autoserpdata_addl").write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/autoserpdata_score_inputs2_autoserpAddl.parquet");

temp_df_1 = sqlctx.sql("(select i.search_term_type,i.term, i.industry, i.search_date, i.location_id, i.rank, i.subrank, i.page, "\
									+ "i.weight,if(i.weight is null , cast(ad.weight as float), cast(i.weight as float)) as weight2, "\
									+ "dom_wgt,goog_wgt, "\
									+ "i.domain,i.hp_count, "\
									+ "i.is_hp,i.is_google,i.is_hp_domain,i.indent_count,i.has_map,i.is_comp,i.is_my_src, "\
									+ "case when i.rating='' or i.rating like '%No%' then cast(0.0 as float) else cast(i.rating as float) end as rating, "\
									+ "i.overall_rating,i.src_base_rating, "\
									+ "case when length(i.reviews)=0 or i.reviews=' ' or i.reviews='' or i.reviews like '%No%' then cast(0.0 as float) else cast(i.reviews as float) end as reviews, "\
									+ "i.num_ratings,i.src_base_reviews, i.src_avg_rank, "\
									+ "i.rating_raw,i.url "\
									+ "from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_inputs.parquet` i "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/location_url_addl3.parquet` ad on i.industry=ad.industry and i.domain=ad.domain and i.country=ad.country_lookup "\
									+ "order by i.industry desc,i.search_date,i.subrank desc,i.location_id) union "\
									+ "(select '',l.name, u.industry, to_date(now()) as dtnow, u.id, -1 as rank, 0 as subrank, -1 as page, "\
									+ "u.weight,u.weight as weight2,u.weight as dom_wgt, 0 as goog_wgt, "\
									+ "u.domain,-1 as hp_count, "\
									+ "0 as is_hp, "\
									+ "case when u.source like '%GOOGLE%' then 1 else 0 end as is_google, "\
									+ "0 as is_hp_domain, "\
									+ "0 as indent_count, "\
									+ "0 as has_map, "\
									+ "u.is_competitor as is_comp, "\
									+ "1 as is_my_src, "\
									+ "case when u.rating='' or u.rating like '%No%' then cast(0.0 as float) else cast(u.rating as float)  end as rating, "\
									+ "cast(u.overall_rating as float) as overall_rating,cast(b.avg_rating as string) as src_base_rating, "\
									+ "case when u.reviews='' or u.reviews='No' then cast(0.0 as float) else cast(u.reviews as float)  end as reviews, "\
									+ "u.num_ratings,b.avg_reviews as src_base_reviews, b.avg_rank as src_avg_rank, "\
									+ "i.rating_raw,u.url "\
									+ "from parquet.`s3n://serp-spark/data-comp-pass/location_url_addl.parquet` u "\
									+ "join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_locations.parquet` sl on u.id=sl.location_id "\
									+ "join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` l on u.id=l.id "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_inputs.parquet` i on u.url=i.url and i.location_id=u.id "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_baselines.parquet` b on u.domain=b.domain and u.industry=b.industry "\
									+ "where i.url is null "\
									+ "and u.weight>=.01 "\
									+ "order by u.industry desc, dtnow ,subrank desc,u.id) union"
									+ "(select '',u.name, i.industry, to_date(now()) as dtnow, u.id, -1 as rank, 0 as subrank, -1 as page, "\
									+ "1 as weight,1 as weight2,0 as dom_wgt, 0 as goog_wgt, "\
									+ "i.domain,-1 as hp_count, "\
									+ "1 as is_hp, "\
									+ "0 as is_google, "\
									+ "1 as is_hp_domain, "\
									+ "0 as indent_count, "\
									+ "0 as has_map, "\
									+ "'' as is_comp, "\
									+ "0 as is_my_src, "\
									+ "cast(-1.0 as float) as rating, "\
									+ "cast(null as float) as overall_rating,cast(-1.0 as string) as src_base_rating, "\
									+ "cast(-1.0 as float) as reviews, "\
									+ "-1 as num_ratings,-1 as src_base_reviews, -1 as src_avg_rank, "\
									+ "i.rating_raw,u.web "\
									+ "from parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` u "\
									+ "join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_locations.parquet` sl on u.id=sl.location_id "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/location_url_addl.parquet` ua on u.id=ua.id "\
									+ "left join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_inputs2_autoserpAddl.parquet` i on trim(u.web)=i.url_scrublastslash and i.tenant_id=u.tenant_id and i.location_code=u.code "\
									+ "where i.url is null "\
									+ "order by i.industry desc, dtnow ,subrank desc,u.id)");

temp_df_1.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/autoserpdata_score_inputs2.parquet");
cur.execute("TRUNCATE TABLE " + outputSchema + ".autoserpdata_score_inputs2");
temp_df_1.write.format("com.databricks.spark.redshift").options(tempdir = tempdir, url = redshift_url, dbtable = "%s.%s" % (outputSchema, "autoserpdata_score_inputs2")).mode("append").save();


create("autoserpdata_tenants", "select tenant_id,count(*) from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata.parquet` group by tenant_id");

create("autoserpdata_industry", "select industry,count(*) from autoserpdata_addl group by industry");

create("autoserpdata_urls_from_vert", "select u.id as location_id,u.source,u.url,u.is_competitor,"\
										+ " sum.num_ratings,sum.num_display_ratings,sum.overall_rating,sum.num_scored_ratings,"\
										+ " now() as ts"\
										+ " from parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location_url1.parquet` u"\
										+ " join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_rating_summaries.parquet` sum on u.url=sum.url"\
										+ " join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` l on u.id=l.id"\
										+ " join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_tenants.parquet` t on t.tenant_id=l.tenant_id");

create("autoserpdata_urls_from_serp_1", "select is_src2, location_id, src, url,"\
										+ " rating,reviews,rank,rating_raw,is_comp,"\
										+ " now() as ts "\
										+ " from autoserpdata_addl");

create("autoserpdata_urls_from_serp", "select location_id, src, url, rating,reviews,rank,rating_raw,is_comp, ts from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_urls_from_serp_1.parquet` where is_src2=1");

create("autoserpdata_urls_from_all", "(select v.location_id,v.source,v.url, "\
									+ " 1 as in_v, case when s.url is null then 0 else 1 end as in_s,"\
									+ " v.overall_rating as rate_v,v.num_ratings as ct_v,"\
									+ " s.rating as rate_s,s.reviews as ct_s,s.rank as rank,"\
									+ " v.is_competitor as is_comp"\
									+ " from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_urls_from_vert.parquet` v"\
									+ " left join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_urls_from_serp.parquet` s on v.location_id=s.location_id and v.url=s.url)"\
									+ " union"\
									+ " (select s.location_id,s.src,s.url,"\
									+ " 0 as in_v, 1 as in_s,"\
									+ " v.overall_rating as rate_v,v.num_ratings as ct_v,"\
									+ " s.rating as rate_s,s.reviews as ct_s,s.rank as rank,"\
									+ " s.is_comp"\
									+ " from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_urls_from_serp.parquet` s"\
									+ " left join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_urls_from_vert.parquet` v on v.location_id=s.location_id and v.url=s.url"\
									+ " where v.url is null)");

create("autoserpdata_score_inputs3", "select i.search_term_type,i.term, i.industry, i.search_date, i.location_id, i.rank, i.subrank, i.page, "\
									+ " i.weight,i.weight2,dom_wgt,goog_wgt,"\
									+ " cast(0.0 as float) as slot_agg,"\
									+ " cast(0.0 as float) as slot_score,"\
									+ " pp.pos_parameter / 2 as w_rank,"\
									+ " greatest((i.is_google+i.has_map+least(1,i.indent_count/4))/4,i.is_hp,is_hp_domain/2) * (i.is_hp*.5 + is_hp_domain*.5 + 3/4*least(i.indent_count/3,2) + 3/4*i.has_map * 2) as w_hp,"\
									+ " greatest((i.is_google+i.has_map)/4,i.is_hp,is_hp_domain/2) * (i.is_hp*.5 + is_hp_domain*.5 + 3/4*i.has_map * 2) as w_map,"\
									+ " greatest(least(1,i.indent_count/4)/4,i.is_hp,is_hp_domain/2) * (i.is_hp*.5 + is_hp_domain*.5 + 3/4*least(i.indent_count/3,2)) as w_indent,"\
									+ " (case when subrank>0 then .5 else 1 end) * if((greatest(dom_wgt,goog_wgt) * 3 + .25) is null,0, greatest(dom_wgt,goog_wgt) * 3 + .25) as w_src,"\
									+ " case when i.weight2 > 0 then greatest(cast(.7 as float),least(cast(1.2 as float),((cast(if(i.reviews is null,0, i.reviews) as float)+10) / 40))) else  "\
									+ " (case when i.is_google=1 then greatest(cast(.7 as float),least(cast(1.2 as float),((cast(if(i.reviews is null,0, i.reviews) as float)+10) / 40))) else 0 end) end as net_ct, "\
									+ " case when (i.weight2 > 0 and i.is_google<>1) then (case when i.rating is null or i.rating=0 then .4 else least(cast(1.2 as float),cast((greatest(cast(-.2 as double),i.rating-.4) / .5) as float)) end) else "\
									+ " (case when i.is_google=1 then (case when if(i.rating is null,i.overall_rating, i.rating) is null or if(i.rating is null,i.overall_rating, i.rating)=0 then .4 else least(cast(1.2 as float),cast((greatest(cast(-.2 as double),if(i.rating is null,i.overall_rating, i.rating)/5.0-.4) / .5) as float)) end) else 0 end) end as net_rating, "\
									+ " case when is_comp='T' then -1 else 0 end + case when is_comp='F' or is_hp_domain=1 then 1 else .7 end as net_me_not_me,"\
									+ " case when is_hp_domain=1 then .2 else (case when weight2>=.01 and is_my_src=0 then 0 else 0 end) end + if(bd.bad_value is null,0, bd.bad_value) * .75 as w_else,"\
									+ " i.domain,"\
									+ " case when i.is_hp=1 then (case when i.rank>0 then 'HP match' else 'HP not match' end)"\
									+ " else (case when i.is_my_src>0 then (case when i.rank>0 then 'src pos' else 'src 0' end) else"\
									+ " (case when weight2>0 then 'src other' else 'else' end) end) end as cat,"\
									+ " case when i.rank>0 then 1 else 0 end as in_serp, i.hp_count,"\
									+ " i.is_hp,i.is_google,i.is_hp_domain,i.indent_count,i.has_map,i.is_comp,"\
									+ " i.rating,"\
									+ " i.overall_rating  as overall_rating,"\
									+ " i.src_base_rating,"\
									+ " i.reviews,i.num_ratings,i.src_base_reviews, i.src_avg_rank,"\
									+ " i.rating_raw,i.url"\
									+ " from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_inputs2.parquet` i"\
									+ " left join parquet.`s3n://serp-spark/data-comp-pass/position_parameter.parquet` pp on i.rank=pp.position"\
									+ " left join parquet.`s3n://serp-spark/data-comp-pass/bad_domains.parquet` bd on i.domain=bd.domain"\
									+ " order by i.term");

update("autoserpdata_score_inputs3", "s", "slot_score", "(w_hp + w_src * net_ct * net_rating) * net_me_not_me");
update("autoserpdata_score_inputs3", "s", "slot_agg", "w_rank * (slot_score + w_else)");

df = sqlctx.sql("select * from autoserpdata_score_inputs3");
df.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/autoserpdata_score_inputs3.parquet");
cur.execute("TRUNCATE TABLE " + outputSchema + ".autoserpdata_score_inputs3");
df.write.format("com.databricks.spark.redshift").options(tempdir = tempdir, url = redshift_url, dbtable = "%s.%s" % (outputSchema, "autoserpdata_score_inputs3")).mode("append").save();


create("autoserpdata_score_agg1", "select search_term_type,l.tenant_id,term, location_id, "\
									+ " i.industry,search_date,domain,is_google, "\
									+ " sum(case when rank>0 then 1 else 0 end) as num_results, "\
									+ " sum(is_hp) as num_hp, "\
									+ " sum(case when w_hp>0 then 1 else 0 end) as ct_like_hp, "\
									+ " max((w_hp + net_me_not_me) * w_rank) as max_hp_score, "\
									+ " max(least(cast(1.5 as double),w_map)) as max_w_map, "\
									+ " max((w_indent + net_me_not_me) * w_rank) as max_w_indent, "\
									+ " max(w_src * net_ct * net_rating * net_me_not_me * w_rank) as max_src_score, "\
									+ " max(w_src * net_ct * net_rating * net_me_not_me * w_rank * is_google) as max_goog_score,  "\
									+ " sum(w_rank * w_else) as w_else, "\
									+ " max(indent_count) as max_indents, "\
									+ " max(has_map) as max_map, "\
									+ " max(hp_count) as hp_count, "\
									+ " sum(is_hp*is_google) as hp_google, "\
									+ " sum(is_hp*is_google*overall_rating) as hp_goog_rating, "\
									+ " sum(is_hp*is_google*reviews) as hp_reviews, "\
									+ " max(case when is_hp=1 then 1 else 0 end) as hp_group, "\
									+ " max(case when net_me_not_me=1 and is_hp_domain=0 then 1 else 0 end) as is_my_src "\
									+ " from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_inputs3.parquet` i "\
									+ " left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` l on i.location_id=l.id "\
									+ " where rank>=0 "\
									+ " group by search_term_type,l.tenant_id,term,location_id,i.industry,search_date,domain,is_google "\
									+ " order by location_id");

drop("sub_table");
create("sub_table", "select id as id_l, web as web_l from parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` l");

drop("joined_table");
create("joined_table", "select * from autoserpdata_score_agg1 s left join sub_table l on (s.location_id=l.id_l and l.web_l is null)");

drop("autoserpdata_score_agg1");
create("autoserpdata_score_agg1", "select search_term_type, tenant_id, term, location_id, industry, search_date, domain, is_google, num_results, num_hp, ct_like_hp, max_hp_score, max_w_map, max_w_indent, max_src_score, max_goog_score,  w_else, max_indents, max_map, hp_count, hp_google, hp_goog_rating, hp_reviews, if(location_id=id_l and web_l is null, -1, trim(hp_group)) as hp_group, is_my_src from joined_table");

df = sqlctx.sql("select * from autoserpdata_score_agg1");
df.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/autoserpdata_score_agg1.parquet");
cur.execute("TRUNCATE TABLE " + outputSchema + ".autoserpdata_score_agg1");
df.write.format("com.databricks.spark.redshift").options(tempdir = tempdir, url = redshift_url, dbtable = "%s.%s" % (outputSchema, "autoserpdata_score_agg1")).mode("append").save();


create("autoserpdata_score_agg", "select search_term_type,i.tenant_id,term, i.location_id, "\
								+ " industry,search_date, "\
								+ " sum(num_results) as num_results, "\
								+ " sum(num_hp) as num_hp, "\
								+ " sum(ct_like_hp) as ct_like_hp, "\
								+ " max(max_hp_score) as max_hp_score, "\
								+ " max(max_w_map) + max(max_w_indent) as max_hp_score2, "\
								+ " sum(case when is_google=0 then max_src_score else 0 end) as sum_src_score, "\
								+ " max(max_goog_score) as max_goog_score,  "\
								+ " sum(w_else) as w_else, "\
								+ " max(max_indents) as max_indents, "\
								+ " max(max_map) as max_map, "\
								+ " sum(hp_google) as hp_google, "\
								+ " sum(hp_goog_rating) as hp_goog_rating, "\
								+ " sum(hp_reviews) as hp_reviews, "\
								+ " max(max_w_map) + max(max_w_indent) + sum(case when is_google=0 then max_src_score else 0 end) + max(max_goog_score) + sum(w_else) as tot_score, "\
								+ " 0 as serp_score_gp,  "\
								+ " 0 as serp_score, "\
								+ " s.score * 1000 as old_rep_score, "\
								+ " s.score * 1000 as new_rep_score, "\
								+ " 0 as score_change, "\
								+ " max(max_w_map) + max(max_w_indent) + sum(case when is_google=0 then abs(max_src_score) else 0 end) + max(abs(max_goog_score)) + sum(abs(w_else)) as tot_abs_score, "\
								+ " case when s.score=0 then 1 else 0 end as flag_no_reviews, "\
								+ " max(hp_group) as hp_group, "\
								+ " sum(case when is_google=0 and max_src_score>0 then 1 else 0 end) as num_srcs, "\
								+ " sum(case when is_google=0 and is_my_src=1 then 1 else 0 end) as num_my_srcs, "\
								+ " 0 as src_diff , "\
								+ " 0 as flag_results "\
								+ " from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_agg1.parquet` i "\
								+ " left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_scores.parquet` s on i.location_id=s.id "\
								+ " group by search_term_type,i.tenant_id,term,i.location_id,industry,search_date,s.score order by i.location_id");

df_score = sqlctx.sql("select * from autoserpdata_score_agg as s");

df_score = df_score.withColumn("src_diff".lower(), expr("num_my_srcs - (num_srcs - num_my_srcs)"));
df_score = df_score.withColumn("flag_results".lower(), expr("case when num_results<11 then 1 else 0 end"));

df_score.registerTempTable("autoserpdata_score_agg");

create("autoserpdata_score_agg_max_abs", "select location_id,max(tot_abs_score) as max_abs_score from autoserpdata_score_agg group by location_id");

df_score1 = sqlctx.sql("select * from autoserpdata_score_agg as s");

df_score1 = df_score1.withColumn("serp_score", expr("case when tot_score is not null then (case when tot_score<1 then 190 + (tot_score-1) * 30 else (case when tot_score<7 then 190 + (tot_score-1) * 110 else 850 + (tot_score-7) * 50 end) end) else null end".lower()));
df_score1 = df_score1.withColumn("serp_score", expr("cast(greatest(0,least(999,serp_score)) as int)".lower()));
df_score1 = df_score1.withColumn("serp_score_gp", expr("round(serp_score,-2)".lower()));
df_score1 = df_score1.withColumn("new_rep_score", expr("old_rep_score * .95 + serp_score * .05".lower()));
df_score1 = df_score1.withColumn("score_change".lower(), expr("cast(new_rep_score - old_rep_score as int)".lower()));

df_score1.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/autoserpdata_score_agg.parquet");
cur.execute("TRUNCATE TABLE " + outputSchema + ".autoserpdata_score_agg");
df_score1.write.format("com.databricks.spark.redshift").options(tempdir = tempdir, url = redshift_url, dbtable = "%s.%s" % (outputSchema, "autoserpdata_score_agg")).mode("append").save();

create("autoserpdata_score_agg_max", "select l.*, m.max_abs_score "\
									+ " from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_agg.parquet` l" \
									+ " join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_agg_max_abs.parquet` m on l.location_id=m.location_id "\
									+ "	and l.tot_abs_score = m.max_abs_score order by l.term");									 

#-------------------------------------------------------------------------------#

create("serp_term_cts","select term, count(*) as ct from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_agg_max.parquet` group by term");#

create("serp_term_tot_srch_cts","select term, count(*) as ct, avg(total_search_count) as total_search_count, min(total_search_count) as min_tsc from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata.parquet` d group by term");#

create("hp_suggest","select sa.tenant_id,sa.location_id, "\
		+ " sa.term,sa.hp_group,i3.is_hp,i3.is_hp_domain,"\
		+ " i3.domain,i3.rank,a1.max_hp_score,i3.url"\
		+ " from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_agg_max.parquet` sa "\
		+ " join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_agg1.parquet` a1 on a1.max_hp_score=sa.max_hp_score and a1.search_term_type=sa.search_term_type and a1.location_id=sa.location_id"\
		+ " join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_inputs3.parquet` i3 on a1.domain=i3.domain and a1.search_term_type=i3.search_term_type and i3.location_id=a1.location_id"\
		+ " where sa.hp_group=-1"\
		+ " order by a1.max_hp_score desc,sa.tenant_id,sa.location_id");


create("location_url_addl4", "select industry,domain,source,country_lookup,avg(weight) as weight from parquet.`s3n://serp-spark/data-comp-pass/location_url_addl.parquet` group by industry,domain,source,country_lookup");


src_suggest1 = hc.sql("select l.id as location_id,l.tenant_id,wc.country as country_lookup,wc.industry,wc.source,wc.weight "\
		+ " from parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` l"\
		+ " left join parquet.`s3n://serp-spark/data-comp-pass/repbiz_sources_weights_calculated.parquet` wc on l.id=wc.id"\
		+ " where wc.weight>=0.01");


#need hivecontext to perform window functions
windowSpec = Window.partitionBy(src_suggest1['location_id']).orderBy(src_suggest1['weight'].desc());
df = rank().over(windowSpec);
src_suggest1 = src_suggest1.select(src_suggest1['location_id'], src_suggest1['tenant_id'], src_suggest1['country_lookup'], src_suggest1['industry'], src_suggest1['source'], src_suggest1['weight'], df.alias("src_rank"));
src_suggest1.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/src_suggest1.parquet");
cur.execute("TRUNCATE TABLE " + outputSchema + ".src_suggest1");
src_suggest1.write.format("com.databricks.spark.redshift").options(tempdir = tempdir, url = redshift_url, dbtable = "%s.%s" % (outputSchema, "src_suggest1")).mode("append").save();

src_indu_rank = hc.sql("select w.*"\
		+ " from parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_sources_weights.parquet` w"\
		+ " where weight>0.01"\
		+ " order by country,industry,weight desc");
windowSpec = Window.partitionBy(src_indu_rank['country']).partitionBy(src_indu_rank['industry']).orderBy(src_indu_rank['weight'].desc());
df = rank().over(windowSpec);
src_indu_rank = src_indu_rank.select(src_indu_rank['id'], src_indu_rank['country'], src_indu_rank['industry'], src_indu_rank['weight'], df.alias("src_rank"));
src_indu_rank.write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/src_indu_rank.parquet");
cur.execute("TRUNCATE TABLE " + outputSchema + ".src_indu_rank");
src_indu_rank.write.format("com.databricks.spark.redshift").options(tempdir = tempdir, url = redshift_url, dbtable = "%s.%s" % (outputSchema, "src_indu_rank")).mode("append").save();



create("src_loc_rank","select l.id as location_id,sir.id as source,"\
		+ " sir.country,sir.industry,sir.weight,sir.src_rank"\
		+ " from parquet.`s3n://serp-spark/data-comp-pass/location_addl.parquet` l"\
		+ " join parquet.`s3n://serp-spark/data-comp-pass/src_indu_rank.parquet` sir on l.industry=sir.industry and l.country=sir.country");#

create("src_suggest2","select s.*,"\
		+ " la.url"\
		+ " from parquet.`s3n://serp-spark/data-comp-pass/src_suggest1.parquet` s"\
		+ " left join parquet.`s3n://serp-spark/data-comp-pass/location_url_addl.parquet` la on s.location_id=la.id and s.source=la.source and la.is_competitor='F'");#

#query out autoserpdata_addl to save operation time
sqlctx.sql("select rank, rating, reviews, google_places_link, url, location_id, domain, search_term_type,term,is_hp, is_hp_domain, has_map, is_my_src, is_google, indent_count, is_bad_domain,top_src from autoserpdata_addl").write.mode("overwrite").save("s3n://serp-spark/data-comp-pass/src_suggest_autoAddl.parquet");
create("src_suggest","select s.tenant_id,s.location_id,l.name as location_name,"\
		+ " s.industry,s.source,s.weight,am.term,a.rank,"\
		+ " la.domain,a.rating,a.reviews,a.google_places_link,a.url"\
		+ " from parquet.`s3n://serp-spark/data-comp-pass/src_suggest2.parquet` s"\
		+ " join parquet.`s3n://serp-spark/data-comp-pass/location_url_addl4.parquet` la on s.industry=la.industry and s.source=la.source and s.country_lookup=la.country_lookup"\
		+ " join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_agg_max.parquet` am on s.location_id=am.location_id"\
		+ " join parquet.`s3n://serp-spark/data-comp-pass/src_suggest_autoAddl.parquet` a on s.location_id=a.location_id and a.domain=la.domain and am.search_term_type=a.search_term_type"\
		+ " join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` l on s.location_id=l.id"\
		+ " where s.url is null");

create("src_suggest_google", "select s.tenant_id,s.location_id,l.name as location_name,"\
	+ " s.industry,s.source,s.weight,am.term,a.rank,"\
	+ " a.rating,a.reviews,a.google_places_link"\
	+ " from parquet.`s3n://serp-spark/data-comp-pass/src_suggest2.parquet` s"\
	+ " join parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_agg_max.parquet` am on s.location_id=am.location_id"\
	+ " join parquet.`s3n://serp-spark/data-comp-pass/src_suggest_autoAddl.parquet` a on s.location_id=a.location_id and am.search_term_type=a.search_term_type"\
	+ " join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_location.parquet` l on s.location_id=l.id"\
	+ " where s.url is null"\
	+ " and a.google_places_link is not null"\
	+ " and s.source='GOOGLE_PLACES'");

create("serp_summary_data", "select a.search_term_type,a.term,a.location_id,"\
		+ " count(*) as ct,"\
		+ " sum(a.is_hp) as sum_is_hp,"\
		+ " sum(a.is_my_src) as sum_my_srcs,"\
		+ " sum(case when (a.is_google=1 or a.domain='google.com') then 1 else 0 end) as sum_google,"\
		+ " sum(case when a.is_hp_domain=1 and (a.is_google=1 or a.domain='google.com') then 1 else 0 end) as sum_google_hp,"\
		+ " min(case when a.is_hp_domain=1 then a.rank else 99 end) as min_hp_rank,"\
		+ " sum(case when a.is_hp_domain=1 and a.is_google=1 and a.has_map=0 then 1 else 0 end) as hp_missing_rhs,"\
		+ " sum(case when a.is_hp_domain=1 and a.indent_count>3 then a.indent_count else 0 end) as hp_indents,"\
		+ " sum(case when a.is_bad_domain=1 then 1 else 0 end) as neg_content_ct,"\
		+ " min(case when a.is_bad_domain=1 then rank else 99 end) as neg_content_worst_rank"\
		+ " from parquet.`s3n://serp-spark/data-comp-pass/src_suggest_autoAddl.parquet` a"\
		+ " group by a.search_term_type,a.term,a.location_id");

create("serp_all_terms","select location_id,term,search_term_type from parquet.`s3n://serp-spark/data/src_suggest_autoAddl.parquet` a group by location_id,term,search_term_type");

create("serp_summary_data_src","select s1.location_id,s1.source,s1.src_rank,s1.url as db_url,t.term,t.search_term_type,"\
		+ " rsum.num_display_ratings,rsum.overall_rating,"\
		+ " a.rank,a.url as serp_url,a.rating,a.reviews,"\
		+ " a.is_my_src"\
		+ " from parquet.`s3n://serp-spark/data-comp-pass/src_suggest2.parquet` s1"\
		+ " join parquet.`s3n://serp-spark/data-comp-pass/serp_all_terms.parquet` t on s1.location_id=t.location_id"\
		+ " left join parquet.`s3n://serp-spark/data-comp-pass/mongo_repbiz_rating_summaries.parquet` rsum on s1.url=rsum.url"\
		+ " left join parquet.`s3n://serp-spark/data-comp-pass/src_suggest_autoAddl.parquet` a on s1.location_id=a.location_id and a.top_src=s1.source and a.rank>=0 and a.search_term_type=t.search_term_type"\
		+ " where s1.src_rank<=2");

create("serp_summary_data_src2","select location_id,source,src_rank,search_term_type,"\
		+ " max(is_my_src) as is_my_src,"\
		+ " avg(case when db_url is null then 1 else 0 end) as no_db_url,"\
		+ " avg(num_display_ratings) as avg_display_ratings,"\
		+ " avg(overall_rating) as avg_rating,"\
		+ " min(case when rank is not null then rank else 99 end) as min_rank"\
		+ " from parquet.`s3n://serp-spark/data-comp-pass/serp_summary_data_src.parquet`"\
		+ " group by location_id,source,src_rank,search_term_type");

create("serp_summary_data_src3","select s1.*, s2.is_my_src as is_my_src_n"\
	+ " from parquet.`s3n://serp-spark/data-comp-pass/serp_summary_data_src.parquet` s1"\
	+ " join parquet.`s3n://serp-spark/data-comp-pass/serp_summary_data_src2.parquet` s2 on s1.location_id=s2.location_id and s1.source=s2.source and s1.src_rank=s2.src_rank and (s1.rank is null or s1.rank=s2.min_rank)");

create("viz_overview", "select sa.location_id,sa.term,sa.search_term_type,"\
					+ " case when sa.sum_src_score=0 and (d.total_search_count>=1000000 or tc.ct>=3) "\
					+ " and (s1.is_my_src=0 and s2.is_my_src=0 and sum.sum_is_hp=0) then 1 else 0 end as broad_term,"\
					+ " case when sa.max_hp_score<=0.7 and sa.num_srcs=0 then 1 else 0 end as bad_term,"\
					+ " sa.hp_group as hp_match,"\
					+ " case when sum.sum_google=0 and (sum.sum_is_hp+sum.sum_my_srcs) > 0 then 1 else 0 end as missing_google,"\
					+ " 0 as google_missing_hp,"\
					+ " sum.min_hp_rank as hp_rank,"\
					+ " sum.hp_missing_rhs,"\
					+ " sum.hp_indents,"\
					+ " sum.neg_content_ct,"\
					+ " sum.neg_content_worst_rank,"\
					+ " s1.source as src1,"\
					+ " case when s1.no_db_url=1 then -1 else 1 end as src1_cat,"\
					+ " s1.min_rank as src1_rank,"\
					+ " s1.avg_rating as src1_rating,"\
					+ " s2.source as src2,"\
					+ " case when s2.no_db_url=1 then -1 else 1 end as src2_cat,"\
					+ " s2.min_rank as src2_rank,"\
					+ " s2.avg_rating as src2_rating,"\
					+ " tc.ct as term_ct,"\
					+ " la.hp_count,"\
					+ " d.total_search_count,"\
					+ " sa.num_hp,sa.max_hp_score,"\
					+ " sa.sum_src_score,sa.max_goog_score,"\
					+ " sa.w_else,sa.max_indents,sa.max_map,"\
					+ " sa.tot_score,sa.tot_abs_score,"\
					+ " sa.serp_score,sa.flag_no_reviews,"\
					+ " sa.hp_group,sa.num_srcs,sa.num_my_srcs,"\
					+ " sa.flag_results"\
					+ " from parquet.`s3n://serp-spark/data-comp-pass/autoserpdata_score_agg_max.parquet` sa "\
					+ " left join parquet.`s3n://serp-spark/data-comp-pass/serp_term_cts.parquet` tc on sa.term=tc.term"\
					+ " left join parquet.`s3n://serp-spark/data-comp-pass/serp_term_tot_srch_cts.parquet` d on sa.term=d.term "\
					+ " left join parquet.`s3n://serp-spark/data-comp-pass/serp_summary_data.parquet` sum on sa.location_id=sum.location_id and sa.search_term_type=sum.search_term_type"\
					+ " left join parquet.`s3n://serp-spark/data-comp-pass/serp_summary_data_src2.parquet` s1 on sa.location_id=s1.location_id and s1.src_rank=1 and sa.search_term_type=s1.search_term_type"\
					+ " left join parquet.`s3n://serp-spark/data-comp-pass/serp_summary_data_src2.parquet` s2 on sa.location_id=s2.location_id and s2.src_rank=2 and sa.search_term_type=s2.search_term_type"\
					+ " left join parquet.`s3n://serp-spark/data-comp-pass/location_addl.parquet` la on sa.location_id=la.id"\
					+ " order by sa.search_term_type desc,broad_term desc");


