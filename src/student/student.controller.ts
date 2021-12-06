import { InjectQueue } from "@nestjs/bull";
import { Controller, Post, UploadedFile, UseInterceptors } from "@nestjs/common";
import { FileInterceptor } from "@nestjs/platform-express/multer";
import { Queue } from "bull";
import { diskStorage } from "multer";
import { editFileName, fileFilter } from "./student-file-upload.-utils";

@Controller('student')
export class StudentController {

  constructor(
    @InjectQueue('create-students') private readonly audioQueue: Queue,
  ) {}

  @Post('uploadFile')
  @UseInterceptors(
    FileInterceptor('file', {
      storage: diskStorage({
        destination: './uploads',
        filename: editFileName,
      }),
      fileFilter: fileFilter,
    }),
  )
  async uploadFile(@UploadedFile() file) {
    await this.audioQueue.add('transcode', {
      file: `./uploads/${file.filename}`,
    });
    const response = {
      status: 'success',
      originalname: file.originalname,
      filename: file.filename,
    };
    return response;
  }  

}

