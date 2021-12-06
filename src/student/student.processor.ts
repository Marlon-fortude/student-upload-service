import { Process, Processor } from "@nestjs/bull";
import { HttpException, InternalServerErrorException, Logger } from "@nestjs/common";
import { ClientProxy, ClientProxyFactory, Transport } from "@nestjs/microservices";
import { WebSocketGateway, WebSocketServer } from "@nestjs/websockets";
import { request, gql } from 'graphql-request';
import { Job } from "bull";
import * as XLSX from 'xlsx';

@WebSocketGateway()
@Processor('create-students')
export class StudentProcessor {

    private readonly logger = new Logger(StudentProcessor.name);
    private client: ClientProxy;

    constructor() {
        this.client = ClientProxyFactory.create({
          transport: Transport.TCP,
        });
    }

    @WebSocketServer()
    server;

    @Process('transcode')
    async handleTranscode(job: Job) {
    this.logger.debug('Converting file tp JSON...');
    const studentsAry = this.readUploadedFile(job.data.file);

    const query = gql`
      mutation createStudentBulk($studentsAry: [CreateStudentInput!]!) {
        createStudentBulk(createStudentInputAry: $studentsAry) {
          id
        }
      }
    `;

      try {
        // //const status = await axios.post("http://localhost:3000/graphql", {
        //   query: query,
        //   variables: {
        //     studentsAry:studentsAryNew
        //   }
        // });
        const bulkStudentUpload = await request("http://localhost:3000/graphql", 
        query, {studentsAry:studentsAry});
        console.log("bulkStudentUpload ", bulkStudentUpload);
        if (bulkStudentUpload.createStudentBulk) {

          this.server.emit('message', 'Upload Complete.');
          this.logger.debug('Process Completed...');
        } else {
          this.logger.log('Error while creating students error=>' + bulkStudentUpload);
          throw new InternalServerErrorException(`Server error, ${bulkStudentUpload}`);
        }
      } catch (err) {
        this.logger.log(
          'Error communicating with graphql server, error=>' + err,
        );
        throw new HttpException(
          `Unexpected error in communication with db server, ${err}`,
          400,
        );
      }
  }

  readUploadedFile(filePath: string) {
    const workbook = XLSX.readFile(filePath, {
      dateNF: 'mm/dd/yyyy',
    });
    const sheet_name_list = workbook.SheetNames;
    return XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]], {
      raw: false,
    });
  }
}