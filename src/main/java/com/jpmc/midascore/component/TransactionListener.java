package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.Incentive;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public TransactionListener(UserRepository userRepository,
                               TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core-group"
    )
    public void listen(Transaction transaction) {

        UserRecord sender =
                userRepository.findById(transaction.getSenderId()).orElse(null);

        UserRecord recipient =
                userRepository.findById(transaction.getRecipientId()).orElse(null);

        // ✅ Validation
        if (sender != null &&
                recipient != null &&
                sender.getBalance() >= transaction.getAmount()) {

            // 🔥 CALL INCENTIVE API
            RestTemplate restTemplate = new RestTemplate();

            String url = "http://localhost:8080/incentive";

            Incentive incentive = restTemplate.postForObject(
                    url,
                    transaction,
                    Incentive.class
            );

            float incentiveAmount = 0.0f;

            if (incentive != null) {
                incentiveAmount = (float) incentive.getAmount();

            }

            // ✅ Update balances correctly
            sender.setBalance(sender.getBalance() - transaction.getAmount());

            recipient.setBalance(
                    recipient.getBalance()
                            + transaction.getAmount()
                            + incentiveAmount
            );

            // Save updated users
            userRepository.save(sender);
            userRepository.save(recipient);

            // Save transaction record
            TransactionRecord record =
                    new TransactionRecord(
                            transaction.getAmount(),
                            sender,
                            recipient
                    );

            transactionRecordRepository.save(record);
            UserRecord wilbur = userRepository.findById(9L).orElse(null);

            if (wilbur != null) {
                System.out.println("WILBUR CURRENT BALANCE: " + wilbur.getBalance());
            }


        }

    }
}


