package com.ksyun.campus.client.entity;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@NoArgsConstructor
@Data
@AllArgsConstructor
public class FileAndMetaData {
    private byte[] fileData;
    private DataServerInfo metaData;
    private DataServerInfo metaData1;
    private DataServerInfo metaData2;
}
