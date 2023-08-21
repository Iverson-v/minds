package com.ksyun.campus.metaserver.entity;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@NoArgsConstructor
@Data
@AllArgsConstructor
public class FileAndMetaData {
    private int length;
    private byte[] fileData;
    private DataServerInfo metaData;
    private DataServerInfo metaData1;
    private DataServerInfo metaData2;
}
