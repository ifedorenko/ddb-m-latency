HEROKU_APP := ifedorenko-adhoc-2021-03-19
HEROKU_DYNO_TYPE := dynamodb-latency

# heroku logs --app=ifedorenko-adhoc-2021-03-19 --dyno=persistent-delta-producer.1 --tail | tee -a ~/tmp/persistent-delta-producer.1.log
# heroku logs --app=ifedorenko-adhoc-2021-03-19 --dyno=persistent-delta-consumer.1 --tail | tee -a ~/tmp/persistent-delta-consumer.1.log

# https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#ConnectToInstance:instanceId=i-05da82ea0f4f8546d
EC2_INSTANCE := ec2-user@ec2-34-226-33-178.compute-1.amazonaws.com

heroku:
	mvn clean package
	docker build --tag=registry.heroku.com/${HEROKU_APP}/${HEROKU_DYNO_TYPE} .
	docker push registry.heroku.com/${HEROKU_APP}/${HEROKU_DYNO_TYPE}
	heroku container:release --app=${HEROKU_APP} ${HEROKU_DYNO_TYPE}

# java -cp /app/ddb-m-latency-1.0-SNAPSHOT.jar adhoc.dynamodb_latency.DynamoDbLatencyOkhttpKt
heroku-oneoff:
	heroku run --size=private-l --app=${HEROKU_APP} --type=${HEROKU_DYNO_TYPE} /bin/bash


ec2:
	mvn clean package
	scp target/ddb-m-latency-*.jar ${EC2_INSTANCE}:/home/ec2-user
