const {kafka} = require('./client');
const group = process.argv[2];
console.log(process.argv);
async function init(){
	const consumer = kafka.consumer({ groupId: group });
	
	await consumer.connect();
	
	await consumer.subscribe({ topics: ['rider-updates'], fromBeginning: true });
	
	await consumer.run({
		eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
			console.log(`${group}: [${topic}: PART:${partition}]`, message.value.toString());
			
			console.log({
				key: message.key.toString(),
				value: message.value.toString(),
				headers: message.headers,
			})
		},
	});	
	
	//await consumer.disconnect();
}

init();