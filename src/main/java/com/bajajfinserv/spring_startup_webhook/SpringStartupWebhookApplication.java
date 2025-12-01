package com.bajajfinserv.spring_startup_webhook;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;
import java.util.*;

@SpringBootApplication
public class SpringStartupWebhookApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(SpringStartupWebhookApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception{
		String name = "Isam Abdul Aziz";
		String regNo = "22BKT0103";
		String email = "isamabdul.aziz2022@vitstudent.ac.in";
		String generateUrl = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";
		String submitUrl = " https://bfhldevapigw.healthrx.co.in/hiring/testWebhook/JAVA";

		RestTemplate rest = new RestTemplate();

		Map<String, String> body = new HashMap<>();
		body.put("name", name);
		body.put("regNo", regNo);
		body.put("email", email);
		System.out.println("Generating webhook");
		GenerateResponse response = rest.postForObject(
				generateUrl,
				body,
				GenerateResponse.class
		);
		if (response == null || response.accessToken == null) {
			System.out.println("didnt receive access token");
			return;
		}
		System.out.println("Received webhook: " + response.webhook);
		System.out.println("Received accessToken: " + response.accessToken);

		// query
		String finalSql =
				"with filtered_payments as \n" +
						"    select p.payment_id, p.emp_id, p.amount, p.payment_time\n" +
						"    from payments p\n" +
						"    where extract(day from p.payment_time) <> 1\n" +
						"),\n" +
						"employee_salary as (\n" +
						"    select e.emp_id, e.first_name, e.last_name, e.dob, e.department,\n" +
						"           sum(fp.amount) as total_salary\n" +
						"    from employee e\n" +
						"    join filtered_payments fp on e.emp_id = fp.emp_id\n" +
						"    group by e.emp_id, e.first_name, e.last_name, e.dob, e.department\n" +
						"),\n" +
						"ranked_salaries as (\n" +
						"    select es.*, d.department_name,\n" +
						"           row_number() over (partition by es.department order by es.total_salary desc) as rn\n" +
						"    from employee_salary es\n" +
						"    join department d on es.department = d.department_id\n" +
						")\n" +
						"select department_name as department_name,\n" +
						"       total_salary as salary,\n" +
						"       concat(first_name, ' ', last_name) as employee_name,\n" +
						"       floor(extract(year from age(dob))) as age\n" +
						"from ranked_salaries\n" +
						"where rn = 1\n" +
						"order by department_name;";

		String targetWebhook = (response.webhook == null || response.webhook.isEmpty())
				? submitUrl
				: response.webhook;

		Map<String, String> submitBody = new HashMap<>();
		submitBody.put("finalQuery", finalSql);
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("Authorization", response.accessToken);
		HttpEntity<Map<String, String>> httpEntity = new HttpEntity<>(submitBody, headers);

		System.out.println("Submitting final query...");
		ResponseEntity<String> submitResponse =
				rest.postForEntity(targetWebhook, httpEntity, String.class);
		System.out.println("Submission Response: " + submitResponse.getStatusCode());
		System.out.println("Body: " + submitResponse.getBody());

		System.out.println("Finished");
	}

}
