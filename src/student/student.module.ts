import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { MulterModule } from '@nestjs/platform-express/multer';
import { StudentController } from './student.controller';

@Module({
    imports: [
        BullModule.registerQueue({
          name: 'create-students',
        }),
        MulterModule.register({
          dest: './uploads',
        }),
      ],
      controllers: [StudentController],
})
export class StudentModule {}
