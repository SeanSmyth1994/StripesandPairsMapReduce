Project was comparing the pairs and stripes co-occurrence approaches. Please note the outputs are just a page sample as they are quite big.
Logs and outputs are in the LogsandOutputs folder

How to run:
	Making the Jar:
		Firstly import the archive file (s3704987.code) into eclipse
		Right click on any of the classes and export it to a runnable jar file
		Upload the jar to Hue.
	 Data Sets (data size tests):
		Create a folder in hue to store the data set(s). I named my cc_input_2019
		use the Hadoop distcp command to put the data into that folder.

hadoop distcp s3a://commoncrawl/crawl-data/CC-MAIN-2015-35/segments/1440644059455.0/wet/CC-MAIN-20150827025419-00000-ip-10-171-96-226.ec2.internal.warc.wet.gz /user/sxxxxx/cc_input_2019
		
		That is the exact command I used to get the data for the first data size test
		sxxxxx refers to your username in hue.
		To upload more data change the “00000” part (before the -ip) to “00001”. And continue
		until you reach for (the max data size I used in these experiments).
		The data used for everything besides the in map combiners was from commoncrawl
		For the in map combiner create another folder in hue. I named mine inmapinput. Then use the Hadoop distcp command again:

hadoop distcp s3a://amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz /user/sxxxxxx/inmapinput

		This was one dataset I used from the amazon reviews dataset. Others included:

hadoop distcp s3a://amazon-reviews-pds/tsv/amazon_reviews_us_Digital_Software_v1_00.tsv.gz  /user/sxxxxxx/inmapinput

hadoop distcp s3a://amazon-reviews-pds/tsv/amazon_reviews_us_Personal_Care_Appliances_v1_00.tsv.gz /user/sxxxxxx/inmapinput

hadoop distcp s3a://amazon-reviews-pds/tsv/amazon_reviews_us_Mobile_Electronics_v1_00.tsv.gz /user/sxxxxxx/inmapinput

hadoop distcp s3a://amazon-reviews-pds/tsv/amazon_reviews_us_Major_Appliances_v1_00.tsv.gz /user/sxxxxxx/inmapinput

		Cluster Tests:
			For the cluster tests (for everything but the in-map combiner) I used half of the full amount I used for the largest data size test (from commoncrawl, from ‘00000’ to ‘00002’).
			I then used the boost cluster.sh command between each test until I reached the max cluster size.
			For the in-map combiner I used the all the data in the inmapinput folder and change the cluster size between each test.

		To run the map reduce:
			No path is hardcoded, choose the name of the output file and run the Hadoop jar command.
			For Example:

			hadoop jar StripeswithPartitioner.jar /user/s3704987/cc_input_2019 /user/s3704987/swp1
			
			Please note that the output file must not be the same as another one you have made. i.e. You cannot use swp1 again, it must be a different name.