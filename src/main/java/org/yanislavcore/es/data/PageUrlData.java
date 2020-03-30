package org.yanislavcore.es.data;

import java.util.Objects;

public class PageUrlData {
    public final String url;
    public final String id;

    public PageUrlData(String url, String id) {
        this.url = url;
        this.id = id;
    }

    @Override
    public String toString() {
        return "PageUrlData{" +
                "url='" + url + '\'' +
                ", id='" + id + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageUrlData that = (PageUrlData) o;
        return Objects.equals(url, that.url) &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, id);
    }
}
