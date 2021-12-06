import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { StudentModule } from './student/student.module';
import { StudentProcessor } from './student/student.processor';

@Module({
  imports: [BullModule.forRoot({
    redis: {
      host: 'localhost',
      port: 6379,
    },
  }),BullModule.registerQueue({
    name: 'create-students',
  }), StudentModule],
  controllers: [],
  providers: [StudentProcessor],
})
export class AppModule {}
